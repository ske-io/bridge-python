import json
import signal
import time
from datetime import UTC, datetime, timedelta

import pytest

from ske_bridge.celery.runner import QueueRunner, _Run, batch_deadline, job_timeout
from tests.conftest import envelope, fixture, record

KEYS = ["message_id", "deleted", "released", "release_delay", "failed", "dead_letter", "attempts", "error"]


@pytest.fixture
def runner(celery_app):
    return QueueRunner(celery_app)


def run(runner, *records, **options):
    return [o.to_dict() for o in runner.process(list(records), options)]


def test_runs_the_lambda_fixture_and_acknowledges_it(runner, celery_app):
    from tests.celery_app import calls

    sqs = fixture("sqs-celery-event.json")["Records"][0]
    # The SQS record as the platform hands it to /__ske/queue.
    queue_record = {
        "message_id": sqs["messageId"],
        "body": sqs["body"],
        "queue": sqs["eventSourceARN"].rsplit(":", 1)[1],
        "attempts": int(sqs["attributes"]["ApproximateReceiveCount"]),
    }

    [outcome] = run(runner, queue_record)

    assert list(outcome) == KEYS
    assert outcome == {
        "message_id": "059f36b4-87a3-44ab-83d2-661975830a7d",
        "deleted": True, "released": False, "release_delay": None,
        "failed": False, "dead_letter": False, "attempts": 1, "error": None,
    }
    assert calls == [("add", 2, 3)]


def test_a_failing_task_is_a_dead_letter(runner, publish):
    [outcome] = run(runner, record(publish("tests.fail")))

    assert outcome["failed"] and outcome["dead_letter"]
    assert not outcome["deleted"] and not outcome["released"]
    assert outcome["error"] == "RuntimeError: it broke"


def test_a_retry_publishes_its_message_and_acknowledges_this_one(runner, publish, sqs_sent):
    body = publish("tests.retry")
    published = len(sqs_sent)

    [outcome] = run(runner, record(body, attempts=1))

    assert outcome["deleted"] and not outcome["released"]
    assert outcome["error"] == "retried: Retry: Retry in 30s: ConnectionError('upstream away')"
    assert len(sqs_sent) == published + 1
    retried = envelope(sqs_sent[-1]["MessageBody"])
    assert retried["headers"]["retries"] == 1
    assert retried["headers"]["id"] == envelope(body)["headers"]["id"]
    eta = datetime.fromisoformat(retried["headers"]["eta"])
    assert 28 <= (eta - datetime.now(UTC)).total_seconds() <= 30


def test_a_retry_keeps_the_arguments_it_was_given(runner, publish, sqs_sent):
    from tests.celery_app import calls

    run(runner, record(publish("tests.retry_args", (1,))))
    retried = sqs_sent[-1]["MessageBody"]
    # Delivered once its countdown has passed.
    env = envelope(retried)
    env["headers"]["eta"] = None
    run(runner, record(json.dumps(env), message_id="again", attempts=1))

    assert calls == [("retry_args", 1, 0), ("retry_args", 2, 1)]
    assert envelope(sqs_sent[-1]["MessageBody"])["headers"]["retries"] == 2


def test_retries_come_from_the_message_not_the_delivery_count(runner, publish):
    from tests.celery_app import calls

    body = publish("tests.retry")

    [outcome] = run(runner, record(body, attempts=7))

    assert outcome["deleted"] and outcome["attempts"] == 1
    assert calls == [("retry", 0)]


def test_a_retry_past_max_retries_fails_for_good(runner, publish):
    env = envelope(publish("tests.retry"))
    env["headers"]["retries"] = 2

    [outcome] = run(runner, record(json.dumps(env)))

    assert outcome["failed"] and outcome["dead_letter"] and outcome["attempts"] == 3


def test_a_retry_that_cannot_be_published_releases_the_message(runner, publish, monkeypatch):
    from tests.conftest import StubSQS

    body = publish("tests.retry")

    def refuse(self, **kwargs):
        raise ConnectionError("SQS unreachable")

    monkeypatch.setattr(StubSQS, "send_message", refuse)
    [outcome] = run(runner, record(body))

    assert outcome["released"] and not outcome["failed"]
    assert "the retry could not be sent" in outcome["error"]


def test_autoretry_for_publishes_its_retry(runner, publish, sqs_sent):
    [outcome] = run(runner, record(publish("tests.autoretry")))

    assert outcome["deleted"]
    assert envelope(sqs_sent[-1]["MessageBody"])["headers"]["retries"] == 1


def test_replace_publishes_the_replacement(runner, publish, sqs_sent):
    from tests.celery_app import calls

    [outcome] = run(runner, record(publish("tests.replace")))

    assert outcome["deleted"]
    assert envelope(sqs_sent[-1]["MessageBody"])["headers"]["task"] == "tests.add"
    assert calls == []


@pytest.mark.parametrize(("requeue", "released"), [(True, True), (False, False)])
def test_reject_releases_only_when_it_requeues(runner, publish, requeue, released):
    [outcome] = run(runner, record(publish("tests.reject", (requeue,))))

    assert outcome["released"] is released
    assert outcome["failed"] is not released


def test_ignore_acknowledges(runner, publish):
    [outcome] = run(runner, record(publish("tests.ignore")))

    assert outcome["deleted"]


def test_a_countdown_still_ahead_is_released_until_then_unrun(runner, publish):
    from tests.celery_app import calls

    [outcome] = run(runner, record(publish("tests.add", (1, 2), countdown=120)))

    assert outcome["released"] and 118 <= outcome["release_delay"] <= 120
    assert outcome["error"] is None
    assert calls == []


def test_a_countdown_past_twelve_hours_is_released_for_twelve_and_checked_again(runner, publish):
    from tests.celery_app import calls

    body = publish("tests.add", (1, 2), countdown=2 * 86400)

    first, second = run(runner, record(body, attempts=1), record(body, attempts=2))

    assert first["release_delay"] == second["release_delay"] == 43200
    assert calls == []


def test_releases_for_a_countdown_do_not_count_as_retries(runner, publish):
    from tests.celery_app import calls

    env = envelope(publish("tests.retry"))

    run(runner, record(json.dumps(env), attempts=4))

    assert calls == [("retry", 0)]


def test_a_countdown_that_passed_runs(runner, publish):
    env = envelope(publish("tests.add", (1, 2)))
    env["headers"]["eta"] = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()

    [outcome] = run(runner, record(json.dumps(env)))

    assert outcome["deleted"]


def test_an_expired_message_is_revoked_unrun(runner, publish, celery_app, monkeypatch):
    from celery import signals

    from tests.celery_app import calls

    env = envelope(publish("tests.add", (1, 2)))
    env["headers"]["expires"] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    task = celery_app.tasks["tests.add"]
    revoked, announced = [], []
    monkeypatch.setattr(task.backend, "mark_as_revoked", lambda task_id, reason, **kw: revoked.append((task_id, reason)), raising=False)

    def on_revoked(sender, request, expired, **kwargs):
        announced.append((sender.name, request.id, expired))

    signals.task_revoked.connect(on_revoked)
    try:
        [outcome] = run(runner, record(json.dumps(env)))
    finally:
        signals.task_revoked.disconnect(on_revoked)

    assert outcome["deleted"] and "expired" in outcome["error"]
    assert revoked == [(env["headers"]["id"], "expired")]
    assert announced == [("tests.add", env["headers"]["id"], True)]
    assert calls == []


def test_a_result_that_cannot_be_saved_after_success_acknowledges(runner, publish, celery_app, monkeypatch):
    from tests.celery_app import calls

    backend = celery_app.tasks["tests.add"].backend

    def down(*args, **kwargs):
        raise ConnectionError("result backend away")

    monkeypatch.setattr(backend, "mark_as_done", down, raising=False)
    monkeypatch.setattr(backend, "mark_as_failure", down, raising=False)

    [outcome] = run(runner, record(publish("tests.add", (1, 2))))

    assert outcome["deleted"] and "saving its result failed" in outcome["error"]
    assert calls == [("add", 1, 2)]


def test_an_unregistered_task_is_a_dead_letter(runner, publish):
    env = envelope(publish("tests.add", (1, 2)))
    env["headers"]["task"] = "gone.task"

    [outcome] = run(runner, record(json.dumps(env)))

    assert outcome["dead_letter"] and "'gone.task' is not registered" in outcome["error"]


def test_a_partial_batch_reports_each_message(runner, publish):
    outcomes = run(
        runner,
        record(publish("tests.add", (1, 1)), message_id="a"),
        record(publish("tests.fail"), message_id="b"),
        record("garbage", message_id="c", attempts=2),
        record(publish("tests.reject", (True,)), message_id="d"),
    )

    assert [(o["message_id"], o["deleted"], o["released"], o["failed"]) for o in outcomes] == [
        ("a", True, False, False),
        ("b", False, False, True),
        ("c", False, False, True),
        ("d", False, True, False),
    ]
    assert outcomes[2]["attempts"] == 2 and outcomes[2]["error"].startswith("unreadable message")


def test_the_queue_timeout_stops_a_task_before_the_function_is_stopped(runner, publish):
    started = time.monotonic()
    [outcome] = run(runner, record(publish("tests.slow", (5,))), timeout=2)

    assert outcome["released"] and "batch's deadline" in outcome["error"]
    assert time.monotonic() - started < 1.5


def test_the_soft_time_limit_raises_soft_time_limit_exceeded(runner, publish):
    [outcome] = run(runner, record(publish("tests.soft")))

    assert outcome["failed"] and outcome["error"].startswith("SoftTimeLimitExceeded")


def test_a_task_that_survives_its_soft_limit_is_stopped_at_its_hard_limit(runner, publish):
    from tests.celery_app import calls

    started = time.monotonic()
    [outcome] = run(runner, record(publish("tests.soft_then_hard")))

    assert outcome["failed"] and outcome["error"].startswith("TimeLimitExceeded")
    assert calls == [("soft",)]
    assert time.monotonic() - started < 1.5


def test_the_batch_shares_one_deadline(runner, publish):
    slow = publish("tests.slow", (5,))
    fast = publish("tests.add", (1, 1))

    started = time.monotonic()
    first, second = run(runner, record(slow, message_id="slow"), record(fast, message_id="fast"), timeout=4)

    # job_timeout(4) is 2s for the whole batch: the first task gets all of
    # it and is stopped by the deadline, not by a limit of its own, so it is
    # released; the second is left too little to start.
    assert first["released"] and not first["failed"] and first["release_delay"] is None
    assert "batch's deadline" in first["error"]
    assert second["released"] and second["error"] == "not started: the batch's time ran out"
    assert time.monotonic() - started < 3


def test_a_healthy_task_late_in_the_batch_is_released_not_dead_lettered(runner, publish):
    from tests.celery_app import calls

    outcomes = run(runner, record(publish("tests.slow", (1.2,)), message_id="a"), record(publish("tests.slow", (3,)), message_id="b"), timeout=4)

    assert outcomes[0]["deleted"]
    assert outcomes[1]["released"] and not outcomes[1]["failed"] and not outcomes[1]["dead_letter"]
    assert calls == []


def test_a_tasks_own_limit_inside_the_deadline_still_fails_it(runner, publish):
    env = envelope(publish("tests.slow", (5,)))
    env["headers"]["timelimit"] = [0.3, None]

    [outcome] = run(runner, record(json.dumps(env)), timeout=60)

    assert outcome["failed"] and outcome["error"].startswith("TimeLimitExceeded")


def test_the_platforms_deadline_wins_over_the_timeout(runner, publish):
    deadline_ms = int((time.time() + 2.6) * 1000)

    started = time.monotonic()
    [outcome] = run(runner, record(publish("tests.slow", (5,))), timeout=900, deadline_ms=deadline_ms)

    # 2.6s left, less min(5, 2.6 / 2) for answering: about 1.3s.
    assert outcome["released"] and "batch's deadline" in outcome["error"]
    assert 1.1 < time.monotonic() - started < 1.6


def test_a_passed_platform_deadline_starts_nothing(runner, publish):
    [outcome] = run(runner, record(publish("tests.add", (1, 1))), deadline_ms=int((time.time() - 1) * 1000))

    assert outcome["released"] and outcome["error"] == "not started: the batch's time ran out"


@pytest.mark.parametrize("options", [{"deadline_ms": "soon"}, {"deadline_ms": True}, {"timeout": True}, {}])
def test_without_a_usable_limit_there_is_no_deadline(options):
    assert batch_deadline(options) is None


def test_the_timeout_counts_from_arrival():
    assert batch_deadline({"timeout": 60}) - time.monotonic() == pytest.approx(55, abs=0.1)


def test_a_task_within_its_limit_finishes(runner, publish):
    [outcome] = run(runner, record(publish("tests.slow", (0.01,))), timeout=60)

    assert outcome["deleted"]


def test_an_alarm_once_the_body_returned_does_nothing():
    import sys

    run_ = _Run(None, 1.0)
    frame = sys._getframe()
    run_.start(frame)
    try:
        run_._on_alarm(signal.SIGALRM, frame)
    finally:
        run_.stop()


def test_a_sigalrm_handler_installed_from_c_leaves_limits_off(runner, publish, monkeypatch):
    monkeypatch.setattr(signal, "getsignal", lambda signum: None)
    armed = []
    monkeypatch.setattr(signal, "setitimer", lambda *a: armed.append(a))

    [outcome] = run(runner, record(publish("tests.slow", (0.01,))), timeout=60)

    assert outcome["deleted"]
    assert armed == []


@pytest.mark.parametrize(("queue", "job"), [(60, 55), (8, 4), (1, 1), (900, 895)])
def test_job_timeout_keeps_a_margin(queue, job):
    assert job_timeout(queue) == job


def test_links_publish_their_next_task(runner, publish, celery_app, sqs_sent):
    celery_app.tasks["tests.add"].s(1, 2).set(link=celery_app.tasks["tests.record"].s()).apply_async()
    first = sqs_sent[-1]["MessageBody"]

    [outcome] = run(runner, record(first))

    assert outcome["deleted"]
    assert envelope(sqs_sent[-1]["MessageBody"])["headers"]["task"] == "tests.record"
