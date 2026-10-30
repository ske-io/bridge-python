import json

import pytest

from ske_bridge.celery.outcome import JobOutcome
from ske_bridge.routes import Routes, is_internal


class FakeRunner:
    def __init__(self):
        self.calls = []

    def process(self, records, options=None):
        self.calls.append((records, options))
        return [JobOutcome.success(r["message_id"], attempts=1) for r in records]


@pytest.fixture
def runner():
    return FakeRunner()


@pytest.fixture
def routes(runner):
    return Routes(lambda: runner)


def queue_body(*ids, **options):
    payload = {"source": "sqs", "records": [{"message_id": i, "body": "{}"} for i in ids]}
    if options:
        payload["options"] = options
    return json.dumps(payload).encode()


@pytest.mark.parametrize(("value", "internal"), [("true", True), ("True", False), ("1", False), ("", False), (None, False), ("true ", False)])
def test_only_the_platforms_exact_header_is_internal(value, internal):
    assert is_internal(value) is internal


def test_the_queue_answers_the_platforms_shape(routes, runner):
    reply = routes.dispatch("POST", "/__ske/queue", "true", queue_body("a", "b", timeout=60, tries=3))

    assert reply.status == 200
    assert reply.body == {
        "processed": 2,
        "outcomes": [
            {"message_id": "a", "deleted": True, "released": False, "release_delay": None, "failed": False, "dead_letter": False, "attempts": 1, "error": None},
            {"message_id": "b", "deleted": True, "released": False, "release_delay": None, "failed": False, "dead_letter": False, "attempts": 1, "error": None},
        ],
    }
    assert runner.calls[0][1] == {"timeout": 60, "tries": 3}


@pytest.mark.parametrize("header", [None, "false", "1"])
def test_the_queue_without_the_internal_header_does_not_exist(routes, runner, header):
    reply = routes.dispatch("POST", "/__ske/queue", header, queue_body("a"))

    assert (reply.status, reply.body) == (404, {"error": "not found"})
    assert runner.calls == []


def test_other_internal_paths_do_not_exist(routes):
    assert routes.dispatch("POST", "/__ske/cli", "true", b"{}").status == 404
    assert routes.dispatch("POST", "/__ske/scheduler", "true", b"{}").status == 404


def test_the_queue_only_takes_post(routes):
    assert routes.dispatch("GET", "/__ske/queue", "true", b"").status == 405


def test_health_answers_without_the_header(routes):
    reply = routes.dispatch("GET", "/__ske/health", None, b"")

    assert (reply.status, reply.body) == (200, {"status": "ok"})


@pytest.mark.parametrize("body", [b"not json", b"[1, 2]"])
def test_an_unreadable_batch_answers_400_so_the_platform_redelivers_it(routes, body):
    assert routes.dispatch("POST", "/__ske/queue", "true", body).status == 400


def test_a_batch_with_no_records_reports_none(routes):
    assert routes.dispatch("POST", "/__ske/queue", "true", b"{}").body == {"processed": 0, "outcomes": []}


def test_an_application_that_cannot_run_jobs_answers_500():
    def broken():
        raise ModuleNotFoundError("No module named 'celery'")

    reply = Routes(broken).dispatch("POST", "/__ske/queue", "true", queue_body("a"))

    assert reply.status == 500
    assert "celery" in reply.body["error"]


def test_routes_handle_only_the_internal_prefix():
    assert Routes.handles("/__ske/queue")
    assert not Routes.handles("/__skeleton")
    assert not Routes.handles("/")
