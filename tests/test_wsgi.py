import io
import json
from wsgiref.util import setup_testing_defaults

import pytest

import ske_bridge.logging
from ske_bridge import wsgi
from ske_bridge.generic import AlreadyIntegrated
from ske_bridge.logging import request_id
from ske_bridge.maintenance import DEFAULT_PAGE
from tests.conftest import record


class App:
    """A plain WSGI app, no framework, that keeps the environ and request id
    each call saw."""

    def __init__(self):
        self.calls = []

    def __call__(self, environ, start_response):
        self.calls.append((dict(environ), request_id.get()))
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]


app = App()


def call(target, path="/", method="GET", headers=None, body=b""):
    environ = {"PATH_INFO": path, "REQUEST_METHOD": method, "REMOTE_ADDR": "127.0.0.1", "wsgi.input": io.BytesIO(body), "CONTENT_LENGTH": str(len(body))}
    for name, value in (headers or {}).items():
        environ["HTTP_" + name.upper().replace("-", "_")] = value
    setup_testing_defaults(environ)

    started = {}

    def start_response(status, response_headers):
        started.update(status=int(status.split(" ", 1)[0]), headers=dict(response_headers))

    content = b"".join(target(environ, start_response))
    return started["status"], started["headers"], content


@pytest.fixture(autouse=True)
def logs_configured(monkeypatch):
    seen = []
    monkeypatch.setattr(ske_bridge.logging, "configure", lambda: seen.append(True))
    return seen


@pytest.fixture
def downstream():
    return App()


@pytest.fixture
def wrapped(platform, downstream):
    return wsgi.wrap(downstream)


def queue(wrapped, records, internal=True):
    headers = {"X-Ske-Internal": "true"} if internal else {}
    return call(wrapped, "/__ske/queue", "POST", headers, json.dumps({"source": "sqs", "records": records}).encode())


def test_the_queue_runs_celery_tasks(wrapped, downstream, publish):
    from tests.celery_app import calls

    status, _, body = queue(wrapped, [record(publish("tests.add", (4, 5)))])

    assert status == 200
    assert json.loads(body)["outcomes"][0]["deleted"]
    assert calls == [("add", 4, 5)]
    assert downstream.calls == []


def test_without_a_celery_app_the_queue_answers_500_naming_what_is_missing(wrapped, monkeypatch):
    monkeypatch.delenv("SKE_CELERY_APP")

    status, _, body = queue(wrapped, [record("{}")])

    assert status == 500
    assert json.loads(body)["error"].startswith("NoQueueRunner: nothing runs queued jobs in this app")


def test_celery_app_names_the_runner_too(wrapped, monkeypatch, publish):
    monkeypatch.delenv("SKE_CELERY_APP")
    monkeypatch.setenv("CELERY_APP", "tests.celery_app:app")

    assert queue(wrapped, [record(publish("tests.add", (1, 1)))])[0] == 200


def test_health_answers_and_other_paths_reach_the_application(wrapped, downstream):
    assert call(wrapped, "/__ske/health")[::2] == (200, b'{"status": "ok"}')
    assert call(wrapped, "/users")[::2] == (200, b"ok")


def test_the_request_id_is_set_around_the_call(wrapped, downstream):
    call(wrapped, headers={"X-Request-Id": "req-1"})

    assert downstream.calls[0][1] == "req-1"


def test_maintenance_answers_503_with_the_default_page(wrapped, downstream, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    monkeypatch.setenv("SKE_MAINTENANCE_BYPASS_TOKEN", "platform-token")

    assert call(wrapped)[::2] == (503, DEFAULT_PAGE)
    assert call(wrapped, headers={"SKE-Maintenance-Bypass": "platform-token"})[0] == 200
    assert len(downstream.calls) == 1


def test_maintenance_answers_head_without_a_body(wrapped, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")

    status, headers, body = call(wrapped, method="HEAD")

    assert (status, body, headers["Content-Length"]) == (503, b"", str(len(DEFAULT_PAGE)))


def test_the_secret_sets_a_cookie_that_lets_the_browser_in(wrapped, downstream, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    monkeypatch.setenv("MAINTENANCE_MODE_SECRET", "let-me-in")

    status, headers, _ = call(wrapped, "/let-me-in", headers={"X-Forwarded-Proto": "https"})

    assert (status, headers["Location"]) == (302, "/")
    assert "Secure" in headers["Set-Cookie"]
    cookie = headers["Set-Cookie"].split(";", 1)[0]
    assert call(wrapped, headers={"Cookie": cookie})[::2] == (200, b"ok")
    assert call(wrapped, headers={"Cookie": "ske_maintenance=forged"})[0] == 503


FORWARDED = {
    "Host": "127.0.0.1:8000",
    "X-Forwarded-For": "203.0.113.9, 198.51.100.7",
    "X-Forwarded-Proto": "https",
    "X-Forwarded-Host": "app.example.com",
    "X-Forwarded-Port": "443",
}


def seen(wrapped, downstream, headers):
    call(wrapped, headers=headers)
    environ = downstream.calls[-1][0]
    return {key: environ[key] for key in ("REMOTE_ADDR", "wsgi.url_scheme", "HTTP_HOST", "SERVER_NAME", "SERVER_PORT")}


def test_the_client_scheme_and_host_are_the_trusted_hops(wrapped, downstream):
    assert seen(wrapped, downstream, FORWARDED) == {
        "REMOTE_ADDR": "198.51.100.7",
        "wsgi.url_scheme": "https",
        "HTTP_HOST": "app.example.com",
        "SERVER_NAME": "app.example.com",
        "SERVER_PORT": "443",
    }


def test_a_non_default_port_stays_in_the_host(wrapped, downstream):
    assert seen(wrapped, downstream, {**FORWARDED, "X-Forwarded-Port": "8443"})["HTTP_HOST"] == "app.example.com:8443"


def test_a_forwarded_port_without_a_forwarded_host_leaves_the_host(wrapped, downstream):
    environ = seen(wrapped, downstream, {"Host": "127.0.0.1:8000", "X-Forwarded-Port": "443"})

    assert environ["HTTP_HOST"] == "127.0.0.1:8000"


def test_without_forwarded_headers_nothing_changes(wrapped, downstream):
    assert seen(wrapped, downstream, {"Host": "127.0.0.1:8000"})["REMOTE_ADDR"] == "127.0.0.1"
    assert seen(wrapped, downstream, {"Host": "127.0.0.1:8000"})["HTTP_HOST"] == "127.0.0.1:8000"


def test_off_the_platform_the_app_is_returned_unchanged(downstream, logs_configured):
    assert wsgi.wrap(downstream) is downstream
    assert logs_configured == []


def test_on_the_platform_logs_are_structured_and_the_app_is_wrapped_once(wrapped, logs_configured):
    assert wsgi.wrap(wrapped) is wrapped
    assert logs_configured == [True]


def test_the_app_comes_from_an_import_string(platform):
    wrapped = wsgi.wrap("tests.test_wsgi:app")

    assert isinstance(wrapped, wsgi.InternalRoutes)
    assert wrapped.app.app is app


def test_off_the_platform_the_import_string_gives_the_app_itself():
    assert wsgi.wrap("tests.test_wsgi:app") is app


@pytest.mark.parametrize("on_platform", [True, False])
def test_a_flask_app_with_the_extension_is_refused(on_platform, monkeypatch):
    from flask import Flask

    from ske_bridge.flask import SkeBridge

    if on_platform:
        monkeypatch.setenv("SKE_ENVIRONMENT", "production")
    flask_app = Flask("tests")
    SkeBridge(flask_app)

    with pytest.raises(AlreadyIntegrated, match="this Flask app has SkeBridge already: serve it as it is"):
        wsgi.wrap(flask_app)


def test_a_flask_app_without_the_extension_is_wrapped(platform):
    from flask import Flask

    assert isinstance(wsgi.wrap(Flask("tests")), wsgi.InternalRoutes)


# queue(): the internal routes alone, for a process that runs jobs


def queue_only(target, path="/__ske/queue", records=(), timeout=None):
    payload = {"source": "sqs", "records": list(records)}
    if timeout is not None:
        payload["options"] = {"timeout": timeout}
    return call(wsgi.queue(target), path, "POST", {"X-Ske-Internal": "true"}, json.dumps(payload).encode())


def test_queue_runs_a_batch_for_an_asgi_app_without_calling_it(platform, publish):
    from tests import test_asgi
    from tests.celery_app import calls

    before = len(test_asgi.app.calls)
    status, _, body = queue_only("tests.test_asgi:app", records=[record(publish("tests.add", (3, 4)))])

    assert status == 200 and json.loads(body)["outcomes"][0]["deleted"]
    assert calls == [("add", 3, 4)]
    assert len(test_asgi.app.calls) == before


def test_queue_enforces_celery_time_limits_on_a_sync_worker(platform, publish):
    import time

    from tests.celery_app import calls

    started = time.monotonic()
    status, _, body = queue_only("tests.test_wsgi:app", records=[record(publish("tests.soft_then_hard"))])

    outcome = json.loads(body)["outcomes"][0]
    assert status == 200 and outcome["failed"] and outcome["error"].startswith("TimeLimitExceeded")
    assert calls == [("soft",)]
    assert time.monotonic() - started < 1.5


def test_queue_enforces_the_batch_deadline_on_a_sync_worker(platform, publish):
    import time

    started = time.monotonic()
    _, _, body = queue_only("tests.test_wsgi:app", records=[record(publish("tests.slow", (5,)))], timeout=2)

    outcome = json.loads(body)["outcomes"][0]
    assert outcome["failed"] and outcome["error"].startswith("TimeLimitExceeded")
    assert time.monotonic() - started < 1.5


def test_queue_answers_health_and_404_for_every_other_path(platform):
    queue = wsgi.queue("tests.test_wsgi:app")
    before = len(app.calls)

    assert call(queue, "/__ske/health")[::2] == (200, b'{"status": "ok"}')
    assert call(queue, "/")[::2] == (404, b'{"error": "not found"}')
    assert call(queue, "/users", "HEAD")[::2] == (404, b"")
    assert len(app.calls) == before


def test_queue_behaves_the_same_off_the_platform(monkeypatch, publish, logs_configured):
    monkeypatch.setenv("SKE_CELERY_APP", "tests.celery_app:app")

    assert queue_only("tests.test_wsgi:app", records=[record(publish("tests.add", (1, 2)))])[0] == 200
    assert call(wsgi.queue("tests.test_wsgi:app"), "/")[0] == 404


def test_queue_without_a_celery_app_answers_500(monkeypatch):
    status, _, body = queue_only("tests.test_wsgi:app", records=[record("{}")])

    assert status == 500 and json.loads(body)["error"].startswith("NoQueueRunner")


def test_queue_refuses_an_app_with_the_framework_integration(monkeypatch):
    import sys

    from flask import Flask

    from ske_bridge.flask import SkeBridge

    flask_app = Flask("tests")
    SkeBridge(flask_app)
    monkeypatch.setattr(sys.modules[__name__], "integrated", flask_app, raising=False)

    with pytest.raises(AlreadyIntegrated, match="this Flask app has SkeBridge already"):
        wsgi.queue("tests.test_wsgi:integrated")


def test_queue_names_an_import_string_that_cannot_be_imported():
    from ske_bridge.generic import AppImportError

    with pytest.raises(AppImportError, match="'nowhere:app'"):
        wsgi.queue("nowhere:app")
