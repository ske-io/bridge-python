import asyncio
import json
import threading
import time
import uuid

import pytest

from ske_bridge.jobs import JobTimeout, job
from ske_bridge.jobs.message import JobMessage, encode
from ske_bridge.logging import request_id
from ske_bridge.queue import QueueRunner
from tests.conftest import record

KEYS = ["message_id", "deleted", "released", "release_delay", "failed", "dead_letter", "attempts", "error"]


def message(name, payload=None, id=None):
    return encode(JobMessage(name, payload, id or str(uuid.uuid4())))


def run(*records, runner=None, **options):
    return [o.to_dict() for o in (runner or QueueRunner()).process(list(records), options)]


@pytest.fixture
def calls():
    return []


@pytest.fixture
def jobs(calls):
    """A few jobs, as an application defines them."""

    @job("add")
    def add(payload, ctx):
        calls.append(("add", payload["a"] + payload["b"], ctx.attempts))

    @job("fail")
    def fail(payload, ctx):
        raise RuntimeError("it broke")

    @job("sleep")
    def sleep(payload, ctx):
        time.sleep(payload)
        calls.append(("slept", payload))

    @job("async-add")
    async def async_add(payload, ctx):
        await asyncio.sleep(0)
        calls.append(("async-add", payload["a"] + payload["b"]))

    @job("async-sleep")
    async def async_sleep(payload, ctx):
        try:
            await asyncio.sleep(payload)
        except asyncio.CancelledError:
            calls.append(("cancelled", ctx.cancelled.is_set()))
            raise


# Outcomes


def test_a_job_that_returns_is_acknowledged(jobs, calls):
    [outcome] = run(record(message("add", {"a": 2, "b": 3}), attempts=2), tries=3)

    assert list(outcome) == KEYS
    assert outcome == {
        "message_id": "msg-1", "deleted": True, "released": False, "release_delay": None,
        "failed": False, "dead_letter": False, "attempts": 2, "error": None,
    }
    assert calls == [("add", 5, 2)]


def test_an_async_job_that_returns_is_acknowledged(jobs, calls):
    [outcome] = run(record(message("async-add", {"a": 1, "b": 1})))

    assert outcome["deleted"]
    assert calls == [("async-add", 2)]


def test_a_failing_job_with_tries_left_is_released_after_the_backoff(jobs):
    [outcome] = run(record(message("fail"), attempts=1), tries=3, backoff=30)

    assert outcome["released"] and outcome["release_delay"] == 30 and not outcome["failed"]
    assert (outcome["attempts"], outcome["error"]) == (1, "RuntimeError: it broke")


def test_a_failing_job_on_its_last_try_is_a_dead_letter(jobs):
    [outcome] = run(record(message("fail"), attempts=3), tries=3, backoff=30)

    assert outcome["failed"] and outcome["dead_letter"] and not outcome["released"]
    assert (outcome["attempts"], outcome["error"]) == (3, "RuntimeError: it broke")


def test_without_tries_a_job_runs_once(jobs):
    [outcome] = run(record(message("fail")))

    assert outcome["failed"] and outcome["dead_letter"]


def test_a_delivery_past_tries_fails_without_running(jobs, calls):
    [outcome] = run(record(message("add", {"a": 1, "b": 1}), attempts=4), tries=3)

    assert outcome["failed"] and outcome["dead_letter"]
    assert outcome["error"] == "delivered 4 times, more than the 3 tries allowed"
    assert calls == []


def test_the_backoff_is_at_most_twelve_hours(jobs):
    [outcome] = run(record(message("fail")), tries=2, backoff=10**6)

    assert outcome["release_delay"] == 43200


def test_an_undefined_job_is_a_dead_letter(jobs):
    [outcome] = run(record(message("gone")), tries=5)

    assert outcome["failed"] and outcome["dead_letter"]
    assert outcome["error"] == "job 'gone' is not defined"


def test_an_invalid_job_message_is_a_dead_letter(jobs):
    [outcome] = run(record(json.dumps({"job": "add", "payload": {}, "id": "not-a-uuid"}), attempts=2), tries=5)

    assert outcome["failed"] and outcome["dead_letter"] and outcome["attempts"] == 2
    assert outcome["error"] == 'unreadable message: "id" is not a UUID'


def test_a_message_that_is_not_a_job_is_a_dead_letter_without_celery(jobs):
    [outcome] = run(record("garbage"), tries=5)

    assert outcome["failed"] and outcome["dead_letter"]
    assert outcome["error"].startswith("unreadable message: not a job message and no Celery app runs it")


def test_an_app_with_no_job_and_no_celery_runs_nothing():
    from ske_bridge.queue import NoQueueRunner

    with pytest.raises(NoQueueRunner, match="define jobs with ske_bridge.jobs"):
        run(record(message("add")))


def test_the_handler_gets_a_context(calls):
    id = str(uuid.uuid4())

    @job("context")
    def context(payload, ctx):
        calls.append((ctx.id, ctx.name, ctx.attempts, ctx.cancelled.is_set(), 0 < ctx.time_left() <= 55))

    run(record(message("context", id=id), attempts=2), tries=2, timeout=60)

    assert calls == [(id, "context", 2, False, True)]


def test_a_partial_batch_reports_each_message(jobs, calls):
    outcomes = run(
        record(message("add", {"a": 1, "b": 1}), message_id="a"),
        record(message("fail"), message_id="b"),
        record(message("gone"), message_id="c"),
        record(message("fail"), message_id="d", attempts=1),
        tries=1,
    )

    assert [(o["message_id"], o["deleted"], o["released"], o["failed"]) for o in outcomes] == [
        ("a", True, False, False),
        ("b", False, False, True),
        ("c", False, False, True),
        ("d", False, False, True),
    ]


def test_handlers_run_with_the_request_id_and_logs(calls, caplog):
    @job("logs")
    def logs(payload, ctx):
        calls.append(request_id.get())
        raise ValueError("bad input")

    token = request_id.set("req-7")
    try:
        with caplog.at_level("ERROR", logger="ske_bridge"):
            [outcome] = run(record(message("logs", id="3f2b8a52-0f3e-4d4f-9d64-6a1d2c3b4a5e")))
    finally:
        request_id.reset(token)

    assert calls == ["req-7"]
    [logged] = [r for r in caplog.records if r.name == "ske_bridge"]
    assert logged.getMessage() == "ske: job logs [3f2b8a52-0f3e-4d4f-9d64-6a1d2c3b4a5e] failed"
    assert isinstance(logged.exc_info[1], ValueError)


def test_around_job_wraps_each_job(jobs, calls):
    from contextlib import contextmanager

    @contextmanager
    def around():
        calls.append("enter")
        try:
            yield
        finally:
            calls.append("exit")

    run(record(message("add", {"a": 1, "b": 2})), record(message("fail")), runner=QueueRunner(around_job=around))

    assert calls == ["enter", ("add", 3, 1), "exit", "enter", "exit"]


# Time limits


def test_a_sync_job_past_its_limit_on_the_main_thread_is_interrupted(jobs, calls):
    started = time.monotonic()
    # timeout=2 leaves the job 1s.
    [outcome] = run(record(message("sleep", 5)), tries=2, backoff=7, timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    assert outcome["released"] and outcome["release_delay"] == 7
    assert outcome["error"].endswith("ran past its 1s time limit")
    assert calls == []
    assert time.monotonic() - started < 1.5


def test_a_sync_job_out_of_time_on_its_last_try_is_a_dead_letter(jobs):
    [outcome] = run(record(message("sleep", 5)), timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    assert outcome["failed"] and outcome["dead_letter"] and "time limit" in outcome["error"]


def test_except_exception_does_not_swallow_the_interruption(calls):
    @job("stubborn")
    def stubborn(payload, ctx):
        try:
            time.sleep(5)
        except Exception:
            calls.append("swallowed")
        calls.append("after")

    started = time.monotonic()
    [outcome] = run(record(message("stubborn")), timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    assert outcome["failed"] and calls == []
    assert time.monotonic() - started < 1.5


def test_a_handler_that_swallows_the_interruption_is_interrupted_again(calls):
    @job("swallows")
    def swallows(payload, ctx):
        try:
            time.sleep(5)
        except JobTimeout:
            calls.append(("caught", ctx.cancelled.is_set()))
        time.sleep(5)

    started = time.monotonic()
    [outcome] = run(record(message("swallows")), timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    assert outcome["failed"] and "time limit" in outcome["error"]
    assert calls == [("caught", True)]
    assert time.monotonic() - started < 2.5


def test_the_alarm_handler_is_put_back(jobs):
    import signal

    before = signal.getsignal(signal.SIGALRM)
    run(record(message("sleep", 5)), timeout=2)
    run(record(message("add", {"a": 1, "b": 1})))

    assert signal.getsignal(signal.SIGALRM) is before
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_an_async_job_past_its_limit_is_cancelled(jobs, calls):
    started = time.monotonic()
    [outcome] = run(record(message("async-sleep", 5)), tries=2, timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    assert outcome["released"] and outcome["error"].endswith("ran past its 1s time limit")
    assert calls == [("cancelled", True)]
    assert time.monotonic() - started < 1.5


def test_an_async_job_that_ignores_its_cancellation_is_left_after_the_grace(calls):
    @job("ignores")
    async def ignores(payload, ctx):
        for _ in range(50):
            try:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                calls.append("ignored")

    started = time.monotonic()
    [outcome] = run(record(message("ignores")), timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    assert outcome["failed"] and "time limit" in outcome["error"]
    assert calls == ["ignored"]
    assert 1.9 < time.monotonic() - started < 2.6


def test_an_async_job_that_holds_the_loop_past_its_limit_is_out_of_time(calls):
    @job("blocks")
    async def blocks(payload, ctx):
        time.sleep(1.3)
        calls.append("returned")

    [outcome] = run(record(message("blocks")), timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    assert outcome["failed"] and "time limit" in outcome["error"]
    assert calls == ["returned"]


def test_off_the_main_thread_a_sync_job_is_told_to_stop_and_counts_as_out_of_time(calls):
    @job("cooperates")
    def cooperates(payload, ctx):
        stopped = ctx.cancelled.wait(5)
        calls.append(("stopped", stopped))

    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.extend(run(record(message("cooperates")), timeout=2, deadline_ms=int((time.time() + 60) * 1000))))
    started = time.monotonic()
    thread.start()
    thread.join(10)

    [outcome] = outcomes
    assert outcome["failed"] and "time limit" in outcome["error"]
    assert calls == [("stopped", True)]
    assert time.monotonic() - started < 1.5


def test_off_the_main_thread_a_job_within_its_limit_is_acknowledged(jobs, calls):
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.extend(run(record(message("sleep", 0.01)), timeout=60)))
    thread.start()
    thread.join(10)

    assert outcomes[0]["deleted"]


def test_a_first_job_that_runs_to_the_batchs_deadline_uses_a_try(jobs):
    # As on the platform: the deadline is the queue's 2s timeout less the
    # platform's own margin for answering, so the batch has 1.8s less
    # min(5, 0.9), 0.9s, just under the job's own 1s. The job had its full
    # time, and running out of it is its own timeout.
    [outcome] = run(record(message("sleep", 5)), tries=2, backoff=7, timeout=2, deadline_ms=int((time.time() + 1.8) * 1000))

    assert outcome["released"] and outcome["release_delay"] == 7 and "keeps_tries" not in outcome
    assert outcome["error"].endswith("time limit")


def test_a_first_job_given_less_than_its_own_limit_by_the_platform_still_uses_a_try(jobs):
    started = time.monotonic()
    # 2.6s left, less min(5, 2.6 / 2) for answering: about 1.3s, under the
    # job's own 895s. Nothing else used the batch's time: that is all the job
    # could have, so it fails on its last try.
    [outcome] = run(record(message("sleep", 5)), timeout=900, deadline_ms=int((time.time() + 2.6) * 1000))

    assert outcome["failed"] and outcome["dead_letter"] and "time limit" in outcome["error"]
    assert 1.1 < time.monotonic() - started < 1.7


def test_a_later_job_cut_short_by_earlier_work_is_released_keeping_its_tries(jobs, calls):
    started = time.monotonic()
    # 3.6s left, less min(5, 1.8): 1.8s for the batch, under the job's own
    # 2s. The first job takes 0.5s of it, so the second is cut short of its
    # full time by the batch, on its last try too.
    first, second = run(
        record(message("sleep", 0.5), message_id="first"),
        record(message("sleep", 5), message_id="second"),
        tries=1, timeout=4, deadline_ms=int((time.time() + 3.6) * 1000),
    )

    assert first["deleted"]
    assert second["released"] and second["release_delay"] is None and second["keeps_tries"] is True
    assert not second["failed"] and second["error"] == "stopped at the batch's deadline"
    assert calls == [("slept", 0.5)]
    assert 1.6 < time.monotonic() - started < 2.4


@pytest.mark.parametrize(("used", "cut"), [(0.5, False), (1.5, True)])
def test_a_job_is_cut_only_when_earlier_work_took_more_than_the_slack(calls, used, cut):
    # Without the platform's deadline the batch has the job time, 55s, from
    # arrival; the slack is a second. A job out of time after less than that
    # of earlier work owns the timeout.
    @job("out-of-time")
    def out_of_time(payload, ctx):
        raise JobTimeout("out of time")

    [outcome] = QueueRunner().process([record(message("out-of-time"))], {"timeout": 60, "tries": 2, "backoff": 3}, time.monotonic() - used)

    if cut:
        assert outcome.released and outcome.release_delay is None and outcome.keeps_tries
        assert outcome.error == "stopped at the batch's deadline"
    else:
        assert outcome.released and outcome.release_delay == 3 and not outcome.keeps_tries


@pytest.mark.parametrize(
    ("left", "own", "whole", "cut"),
    [
        # Full time is the job's own 10s; the slack is 1s.
        (9.0, 10.0, 20.0, False),
        (8.999, 10.0, 20.0, True),
        # On the platform the batch is a little shorter than the job's own
        # limit: full time is the batch's 54s.
        (54.0, 55.0, 54.0, False),
        (53.0, 55.0, 54.0, False),
        (52.999, 55.0, 54.0, True),
        # No limit of its own: the batch's whole time; a tenth of a short one.
        (0.45, None, 0.5, False),
        (0.449, None, 0.5, True),
    ],
)
def test_the_cut_rule_boundary(left, own, whole, cut):
    from ske_bridge.queue import cut_by_batch

    assert cut_by_batch(left, own, whole) is cut


def test_the_batch_shares_one_deadline(jobs, calls):
    started = time.monotonic()
    first, second = run(
        record(message("sleep", 5), message_id="slow"),
        record(message("add", {"a": 1, "b": 1}), message_id="fast"),
        timeout=60, deadline_ms=int((time.time() + 4) * 1000),
    )

    # 4s left, less min(5, 4 / 2): 2s for the whole batch. The first job gets
    # all of it, its full time, and fails on its own timeout; the second is
    # left no time to start, and keeps its tries.
    assert first["failed"] and "time limit" in first["error"]
    assert second["released"] and second["error"] == "not started: the batch's time ran out"
    assert second["release_delay"] is None and second["keeps_tries"] is True
    assert calls == []
    assert time.monotonic() - started < 3


def test_a_passed_platform_deadline_starts_nothing(jobs, calls):
    [outcome] = run(record(message("add", {"a": 1, "b": 1})), deadline_ms=int((time.time() - 1) * 1000))

    assert outcome["released"] and outcome["error"] == "not started: the batch's time ran out"
    assert calls == []


def test_without_the_platforms_deadline_the_first_job_has_its_own_limit(jobs):
    # The cutoff is the job time from arrival, a moment before the job
    # starts: running past it is the job's own timeout, which uses a try.
    [outcome] = run(record(message("sleep", 5)), tries=2, backoff=4, timeout=2)

    assert outcome["released"] and outcome["release_delay"] == 4
    assert outcome["error"].endswith("ran past its 1s time limit")


def test_a_release_for_the_batchs_time_keeps_the_jobs_tries(jobs, calls):
    # A job left unstarted is released keeping its tries: the platform
    # delivers it again with the same attempts, so with tries=1 it still
    # runs on that delivery.
    body = message("add", {"a": 1, "b": 1})
    first, released = run(
        record(message("sleep", 5), message_id="slow"),
        record(body, message_id="later", attempts=1),
        tries=1, timeout=60, deadline_ms=int((time.time() + 2) * 1000),
    )
    assert released["released"] and released["error"] == "not started: the batch's time ran out"
    assert released["release_delay"] is None and released["keeps_tries"] is True

    [redelivered] = run(record(body, message_id="later", attempts=1), tries=1)

    assert redelivered["deleted"]
    assert calls == [("add", 2, 1)]


def test_a_counted_release_does_not_say_it_keeps_tries(jobs):
    [outcome] = run(record(message("fail")), tries=2, backoff=0)

    assert outcome["released"] and outcome["release_delay"] == 0 and "keeps_tries" not in outcome


def test_an_async_job_holding_the_loop_on_the_main_thread_is_interrupted(calls):
    @job("holds")
    async def holds(payload, ctx):
        try:
            time.sleep(10)
        except JobTimeout:
            calls.append(("interrupted", ctx.cancelled.is_set()))
            raise

    started = time.monotonic()
    [outcome] = run(record(message("holds")), timeout=2, deadline_ms=int((time.time() + 60) * 1000))

    # 1s limit, 1s grace, then the alarm.
    assert outcome["failed"] and "time limit" in outcome["error"]
    assert calls == [("interrupted", True)]
    assert 2 < time.monotonic() - started < 2.6


def test_the_batchs_time_counts_from_the_requests_arrival(jobs, calls):
    # Arrived 1.5s ago on a 4s queue: 0.5s of its 2s job time is left.
    [outcome] = QueueRunner().process([record(message("add", {"a": 1, "b": 1}))], {"timeout": 4}, time.monotonic() - 1.5)
    assert outcome.deleted

    [outcome] = QueueRunner().process([record(message("add", {"a": 1, "b": 1}))], {"timeout": 4}, time.monotonic() - 2.5)
    assert outcome.released and outcome.error == "not started: the batch's time ran out"
    assert calls == [("add", 2, 1)]


def test_a_fractional_deadline_ms_is_the_platforms_deadline(jobs):
    from ske_bridge.jobs.runner import JobRunner
    from ske_bridge.queue import batch_deadline

    options = {"timeout": 900, "deadline_ms": (time.time() + 10) * 1000 + 0.5}
    received = time.monotonic()
    policy = JobRunner(None).policy(options, received)

    assert policy.cutoff == pytest.approx(batch_deadline(options, received), abs=0.01)
    assert policy.cutoff - received == pytest.approx(5, abs=0.1)
    assert policy.end - received == pytest.approx(10, abs=0.1)
