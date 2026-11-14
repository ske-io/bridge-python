import json
import time
import uuid

import pytest

from ske_bridge import generic
from ske_bridge.jobs import job
from ske_bridge.jobs.message import JobMessage, encode
from ske_bridge.queue import NoQueueRunner, QueueRunner
from tests.conftest import record


def message(name, payload=None):
    return encode(JobMessage(name, payload, str(uuid.uuid4())))


@pytest.fixture
def ran():
    return []


@pytest.fixture
def jobs(ran):
    @job("add")
    def add(payload, ctx):
        ran.append(("job", payload["a"] + payload["b"]))

    @job("sleep")
    def sleep(payload, ctx):
        time.sleep(payload)


def run(runner, *records, **options):
    return [o.to_dict() for o in runner.process(list(records), options)]


def test_a_mixed_batch_runs_jobs_and_celery_tasks_in_order(platform, jobs, ran, publish):
    from tests.celery_app import calls

    outcomes = run(
        generic.queue_runner(),
        record(message("add", {"a": 1, "b": 2}), message_id="job"),
        record(publish("tests.add", (3, 4)), message_id="task"),
        record(publish("tests.fail"), message_id="failing-task"),
        record(message("gone"), message_id="undefined-job"),
    )

    assert [(o["message_id"], o["deleted"], o["failed"]) for o in outcomes] == [
        ("job", True, False), ("task", True, False), ("failing-task", False, True), ("undefined-job", False, True),
    ]
    assert outcomes[3]["error"] == "job 'gone' is not defined"
    assert ran == [("job", 3)]
    assert calls == [("add", 3, 4)]


def test_a_raw_json_celery_envelope_is_not_taken_for_a_job(platform, jobs, publish):
    from tests.conftest import envelope

    [outcome] = run(generic.queue_runner(), record(json.dumps(envelope(publish("tests.add", (1, 1))))))

    assert outcome["deleted"]


def test_jobs_and_tasks_share_the_batchs_deadline(platform, jobs, publish):
    started = time.monotonic()
    first, second = run(
        generic.queue_runner(),
        record(message("sleep", 5), message_id="job"),
        record(publish("tests.add", (1, 1)), message_id="task"),
        timeout=60, deadline_ms=int((time.time() + 4) * 1000),
    )

    # The job had the batch's whole time, its full time: it fails on its own
    # timeout. The task never started, so it keeps its tries.
    assert first["failed"] and "time limit" in first["error"]
    assert second["released"] and second["error"] == "not started: the batch's time ran out"
    assert second["keeps_tries"] is True
    assert time.monotonic() - started < 3


def test_a_batch_of_jobs_needs_no_celery_app(jobs, ran):
    [outcome] = run(generic.queue_runner(), record(message("add", {"a": 1, "b": 1})))

    assert outcome["deleted"] and ran == [("job", 2)]


def test_a_celery_message_without_a_celery_app_runs_nothing_of_the_batch(jobs, ran, publish):
    with pytest.raises(NoQueueRunner):
        generic.queue_runner().process([record(message("add", {"a": 1, "b": 1})), record(publish("tests.add", (1, 1)))])

    assert ran == []


def test_a_celery_app_that_cannot_be_found_runs_nothing_of_the_batch(jobs, ran, publish):
    from ske_bridge.celery import NoCeleryApp

    def missing():
        raise NoCeleryApp("2 Celery apps exist")

    with pytest.raises(NoCeleryApp):
        QueueRunner(celery=missing).process([record(message("add", {"a": 1, "b": 1})), record(publish("tests.add", (1, 1)))])

    assert ran == []


def test_a_celery_only_app_still_needs_its_celery_app(publish):
    with pytest.raises(NoQueueRunner, match="set SKE_CELERY_APP"):
        generic.queue_runner().process([record(publish("tests.add", (1, 1)))])


def test_a_job_message_in_a_celery_only_app_is_a_dead_letter(platform, publish):
    [outcome] = run(generic.queue_runner(), record(message("add")))

    assert outcome["failed"] and outcome["error"] == "job 'add' is not defined"


def test_the_celery_runner_is_made_once(platform, jobs, publish):
    made = []

    def celery():
        made.append(True)
        return generic.celery_runner()

    runner = QueueRunner(celery=celery)
    run(runner, record(publish("tests.add", (1, 1))))
    run(runner, record(publish("tests.add", (1, 1))))

    assert made == [True]


def test_wsgi_queue_interrupts_a_sync_job_on_the_main_thread(platform, jobs):
    from ske_bridge import wsgi
    from tests.test_wsgi import call

    started = time.monotonic()
    body = json.dumps({"source": "sqs", "records": [record(message("sleep", 5))], "options": {"timeout": 2, "tries": 2, "backoff": 3}})
    status, _, reply = call(wsgi.queue("tests.test_wsgi:app"), "/__ske/queue", "POST", {"X-Ske-Internal": "true"}, body.encode())

    outcome = json.loads(reply)["outcomes"][0]
    assert status == 200 and outcome["released"] and outcome["release_delay"] == 3
    assert outcome["error"].endswith("ran past its 1s time limit")
    assert time.monotonic() - started < 1.5


def test_an_asgi_app_runs_jobs_in_its_queue_thread(platform, jobs, ran):
    from ske_bridge import asgi
    from tests.test_asgi import App, queue

    status, _, body = queue(asgi.wrap(App()), [record(message("add", {"a": 2, "b": 2}))])

    assert status == 200 and json.loads(body)["outcomes"][0]["deleted"]
    assert ran == [("job", 4)]


def test_a_job_interrupted_on_the_main_thread_leaves_a_celery_tasks_limits_working(platform, jobs, publish):
    import signal

    from tests.celery_app import calls

    before = signal.getsignal(signal.SIGALRM)
    started = time.monotonic()
    first, second = run(
        generic.queue_runner(),
        record(message("sleep", 5), message_id="job"),
        record(publish("tests.soft_then_hard"), message_id="task"),
        timeout=4, tries=2, deadline_ms=int((time.time() + 60) * 1000),
    )

    # The job's own 2s limit stops it; the task then hits its soft and hard
    # limits (0.2s, 0.5s) through its own alarm.
    assert first["released"] and first["error"].endswith("ran past its 2s time limit")
    assert second["failed"] and second["error"].startswith("TimeLimitExceeded")
    assert calls == [("soft",)]
    assert time.monotonic() - started < 3.2
    assert signal.getsignal(signal.SIGALRM) is before


def test_concurrent_asgi_batches_mixing_jobs_and_tasks_keep_apart(platform, celery_app, publish):
    import asyncio
    import threading

    from ske_bridge import asgi
    from tests.test_asgi import App, queue_call, response

    barrier = threading.Barrier(2, timeout=5)
    seen = []

    @job("meet")
    def meet(payload, ctx):
        barrier.wait()
        seen.append((payload, ctx.name, threading.current_thread().name.startswith("ske-queue")))

    @celery_app.task(name="tests.mixed")
    def mixed(value):
        seen.append(("task", value))

    wrapped = asgi.wrap(App())

    async def both():
        return await asyncio.gather(
            queue_call(wrapped, [record(message("meet", "a"), message_id="a-job"), record(publish("tests.mixed", ("a",)), message_id="a-task")]),
            queue_call(wrapped, [record(publish("tests.mixed", ("b",)), message_id="b-task"), record(message("meet", "b"), message_id="b-job")]),
        )

    first, second = (json.loads(response(sent)[2])["outcomes"] for sent in asyncio.run(both()))

    assert [(o["message_id"], o["deleted"]) for o in first] == [("a-job", True), ("a-task", True)]
    assert [(o["message_id"], o["deleted"]) for o in second] == [("b-task", True), ("b-job", True)]
    assert sorted(s for s in seen if s[0] in ("a", "b")) == [("a", "meet", True), ("b", "meet", True)]
    assert sorted(s for s in seen if s[0] == "task") == [("task", "a"), ("task", "b")]
