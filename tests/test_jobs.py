import asyncio
import base64
import io
import json
import threading
import uuid

import pytest

from ske_bridge.jobs import Job, JobContext, dispatch, job
from ske_bridge.jobs.message import InvalidMessage, JobMessage, decode, encode, is_job_message


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("SKE_ENVIRONMENT", "production")
    monkeypatch.setenv("SQS_PREFIX", "https://sqs.ca-central-1.amazonaws.com/123456789012/")
    monkeypatch.setenv("SQS_SUFFIX", "--shop-production")


@pytest.fixture
def seen():
    return []


@pytest.fixture
def welcome(seen):
    @job("send-welcome-mail")
    def send_welcome(payload, ctx):
        seen.append((payload, ctx))

    return send_welcome


# Defining


def test_job_defines_a_named_job(welcome, defined_jobs):
    assert isinstance(welcome, Job)
    assert welcome.name == "send-welcome-mail"
    assert defined_jobs == {"send-welcome-mail": welcome}


def test_a_job_can_be_called_directly(welcome, seen):
    ctx = JobContext(id="id", name="send-welcome-mail", attempts=1)

    welcome({"user_id": 7}, ctx)

    assert seen == [({"user_id": 7}, ctx)]


def test_defining_a_name_twice_is_an_error(welcome):
    with pytest.raises(ValueError, match="a job named 'send-welcome-mail' is already defined"):
        job("send-welcome-mail")(lambda payload, ctx: None)


@pytest.mark.parametrize("name", ["", None, 7])
def test_a_job_needs_a_name(name):
    with pytest.raises(TypeError, match="a job needs a name"):
        job(name)


def test_a_job_needs_a_handler():
    with pytest.raises(TypeError, match="needs a handler function"):
        job("nothing")("not callable")


# Dispatching on the platform


def test_dispatch_sends_the_message_to_the_default_queue(aws, welcome, jobs_sent, seen):
    id = welcome.dispatch({"user_id": 7})

    assert uuid.UUID(id).version == 4
    [sent] = jobs_sent
    assert sent == {
        "QueueUrl": "https://sqs.ca-central-1.amazonaws.com/123456789012/default--shop-production",
        "MessageBody": json.dumps({"job": "send-welcome-mail", "payload": {"user_id": 7}, "id": id}, separators=(",", ":")),
    }
    assert seen == []


def test_dispatch_by_name_with_a_delay_and_a_queue(aws, welcome, jobs_sent, monkeypatch):
    monkeypatch.setenv("SQS_QUEUES", "default,emails")

    dispatch("send-welcome-mail", {"user_id": 7}, delay=900, queue="emails")

    [sent] = jobs_sent
    assert sent["QueueUrl"].endswith("/emails--shop-production")
    assert sent["DelaySeconds"] == 900


def test_the_default_queue_is_sqs_queue(aws, welcome, jobs_sent, monkeypatch):
    monkeypatch.setenv("SQS_QUEUE", "main")

    welcome.dispatch()

    assert jobs_sent[0]["QueueUrl"].endswith("/main--shop-production")
    assert json.loads(jobs_sent[0]["MessageBody"])["payload"] is None


def test_a_queue_the_environment_does_not_list_is_refused(aws, welcome, jobs_sent, monkeypatch):
    monkeypatch.setenv("SQS_QUEUES", "default,emails")

    with pytest.raises(ValueError, match="the environment has no queue named 'reports'; its queues are default, emails"):
        welcome.dispatch({}, queue="reports")
    assert jobs_sent == []


def test_without_sqs_prefix_there_is_no_queue(aws, welcome, jobs_sent, monkeypatch):
    monkeypatch.delenv("SQS_PREFIX")

    with pytest.raises(RuntimeError, match="SQS_PREFIX is not set"):
        welcome.dispatch({})


@pytest.mark.parametrize("delay", [-1, 901, 1.5, True, "5"])
def test_a_delay_is_whole_seconds_up_to_900(aws, welcome, jobs_sent, delay):
    with pytest.raises(ValueError, match="whole number of seconds from 0 to 900"):
        welcome.dispatch({}, delay=delay)
    assert jobs_sent == []


@pytest.mark.parametrize("payload", [{"when": __import__("datetime").date(2026, 1, 1)}, {"x": float("nan")}, {1, 2}])
def test_a_payload_that_is_not_json_is_refused(aws, welcome, jobs_sent, payload):
    with pytest.raises(TypeError, match="the payload of job 'send-welcome-mail' is not a JSON value"):
        welcome.dispatch(payload)
    assert jobs_sent == []


def test_an_undefined_job_is_not_dispatched(aws, jobs_sent):
    with pytest.raises(LookupError, match="no job named 'nowhere' is defined"):
        dispatch("nowhere", {})
    assert jobs_sent == []


def test_a_refused_send_raises_to_the_caller(aws, welcome, monkeypatch):
    import ske_bridge.jobs.send

    class Refusing:
        def send_message(self, **kwargs):
            raise ConnectionError("SQS unreachable")

    monkeypatch.setattr(ske_bridge.jobs.send, "sqs_client", lambda: Refusing())

    with pytest.raises(ConnectionError, match="SQS unreachable"):
        welcome.dispatch({})


def test_the_sqs_client_is_made_once_per_region(aws, monkeypatch):
    import boto3

    import ske_bridge.jobs.send as send

    made = []
    monkeypatch.setattr(send, "_sqs_clients", {})
    monkeypatch.setattr(boto3, "client", lambda service, region_name=None: made.append((service, region_name)) or object())
    monkeypatch.setenv("AWS_REGION", "ca-central-1")

    first, second = send.sqs_client(), send.sqs_client()

    assert first is second
    assert made == [("sqs", "ca-central-1")]


# Dispatching on GCP


@pytest.fixture
def gcp(monkeypatch):
    import ske_bridge.cloudtasks as cloudtasks

    monkeypatch.setenv("SKE_ENVIRONMENT", "production")
    monkeypatch.setenv("SKE_PROVIDER", "gcp")
    monkeypatch.setenv("SKE_GCP_PROJECT", "test-project")
    monkeypatch.setenv("SKE_REGION", "us-central1")
    monkeypatch.setenv("SKE_QUEUE_PREFIX", "ske-shop-prod--")
    monkeypatch.setenv("SKE_QUEUES", "default,emails")
    sent, tokens = [], []

    def request(req, timeout):
        if req.full_url.startswith("http://metadata.google.internal/"):
            assert req.get_header("Metadata-flavor") == "Google"
            tokens.append(req)
            return io.BytesIO(b'{"access_token":"test-token","expires_in":3600}')
        assert req.get_header("Authorization") == "Bearer test-token"
        sent.append((req.full_url, json.loads(req.data)["task"]))
        return io.BytesIO(b"{}")

    monkeypatch.setattr(cloudtasks, "urlopen", request)
    return sent, tokens


def test_on_gcp_dispatch_creates_a_cloud_task(gcp, welcome):
    sent, tokens = gcp

    id = welcome.dispatch({"user_id": 7}, queue="emails")
    welcome.dispatch({"user_id": 8}, delay=60)

    (first_url, first), (second_url, second) = sent
    assert first_url == "https://cloudtasks.googleapis.com/v2/projects/test-project/locations/us-central1/queues/ske-shop-prod--emails/tasks"
    assert second_url.endswith("/queues/ske-shop-prod--default/tasks")
    assert json.loads(base64.b64decode(first["httpRequest"]["body"])) == {"job": "send-welcome-mail", "payload": {"user_id": 7}, "id": id}
    assert "scheduleTime" not in first and "scheduleTime" in second
    assert len(tokens) == 1


def test_on_gcp_a_queue_the_environment_does_not_list_is_refused(gcp, welcome):
    with pytest.raises(ValueError, match="no queue named 'reports'"):
        welcome.dispatch({}, queue="reports")
    assert gcp[0] == []


# Dispatching off the platform


def test_off_the_platform_sync_mode_runs_the_handler_before_dispatch_returns(welcome, seen, monkeypatch, jobs_sent):
    monkeypatch.setenv("SKE_QUEUE_SYNC", "1")

    id = welcome.dispatch({"user_id": 7, "tags": ("a", "b")})

    [(payload, ctx)] = seen
    # A JSON copy, as a delivered job gets.
    assert payload == {"user_id": 7, "tags": ["a", "b"]}
    assert (ctx.id, ctx.name, ctx.attempts, ctx.cancelled.is_set(), ctx.time_left()) == (id, "send-welcome-mail", 1, False, None)
    assert jobs_sent == []


def test_off_the_platform_sync_mode_raises_what_the_handler_raised(monkeypatch):
    monkeypatch.setenv("SKE_QUEUE_SYNC", "1")

    @job("broken")
    def broken(payload, ctx):
        raise RuntimeError("it broke")

    with pytest.raises(RuntimeError, match="it broke"):
        broken.dispatch({})


def test_off_the_platform_sync_mode_runs_an_async_handler(monkeypatch, seen):
    monkeypatch.setenv("SKE_QUEUE_SYNC", "1")

    @job("async")
    async def handler(payload, ctx):
        await asyncio.sleep(0)
        seen.append(payload)

    handler.dispatch(1)

    async def from_running_loop():
        handler.dispatch(2)

    asyncio.run(from_running_loop())

    assert seen == [1, 2]


def test_off_the_platform_the_handler_runs_in_this_process_after_the_delay(seen):
    ran = threading.Event()

    @job("later")
    def later(payload, ctx):
        seen.append((payload, threading.current_thread() is threading.main_thread()))
        ran.set()

    later.dispatch({"n": 1})

    assert ran.wait(5)
    assert seen == [({"n": 1}, False)]


def test_off_the_platform_a_delay_is_waited(seen):
    import time

    ran = threading.Event()

    @job("delayed")
    def delayed(payload, ctx):
        ran.set()

    started = time.monotonic()
    delayed.dispatch(None, delay=1)

    assert not ran.wait(0.5)
    assert ran.wait(5)
    assert time.monotonic() - started >= 1


def test_off_the_platform_a_failing_job_is_logged(caplog):
    done = threading.Event()

    @job("fails-later")
    def fails(payload, ctx):
        done.set()
        raise RuntimeError("it broke")

    with caplog.at_level("ERROR", logger="ske_bridge"):
        id = fails.dispatch({})
        assert done.wait(5)
        for _ in range(100):
            if caplog.records:
                break
            threading.Event().wait(0.02)

    [logged] = caplog.records
    assert logged.getMessage() == f"ske: job fails-later [{id}] failed"
    assert str(logged.exc_info[1]) == "it broke"


# The message


def test_the_message_round_trips():
    message = JobMessage("send-welcome-mail", {"name": "Zoë"}, str(uuid.uuid4()))

    body = encode(message)

    assert body == '{"job":"send-welcome-mail","payload":{"name":"Zoë"},"id":"' + message.id + '"}'
    assert decode(body) == message
    assert is_job_message(body)


@pytest.mark.parametrize(("body", "reason"), [
    (None, "the body is not text"),
    ("nope", "the body is not JSON"),
    ("[1]", "the body is not a JSON object"),
    ('{"job":"","payload":1,"id":"' + str(uuid.uuid4()) + '"}', '"job" is not a job name'),
    ('{"job":"a","id":"' + str(uuid.uuid4()) + '"}', '"payload" is missing'),
    ('{"job":"a","payload":1,"id":"7"}', '"id" is not a UUID'),
])
def test_a_body_that_is_not_a_job_message_is_refused(body, reason):
    with pytest.raises(InvalidMessage, match=reason):
        decode(body)


@pytest.mark.parametrize(("body", "job_message"), [
    ('{"job":"a","payload":1,"id":"x"}', True),
    (' {"job": 1}', True),
    ('{"body":"...","headers":{"task":"tests.add"}}', False),
    (base64.b64encode(b'{"job":"a"}').decode(), False),
    ("{not json", False),
    (None, False),
])
def test_a_job_message_is_a_json_object_with_a_job_key(body, job_message):
    assert is_job_message(body) is job_message


def test_the_same_function_defined_again_replaces_the_first():
    def define():
        @job("reloaded")
        def handler(payload, ctx):
            return "second"

        return handler

    first = define()
    second = define()

    assert first is second and first(None, None) == "second"


def test_a_different_function_under_a_taken_name_is_an_error():
    job("taken")(lambda payload, ctx: None)

    def other(payload, ctx):
        pass

    with pytest.raises(ValueError, match="already defined"):
        job("taken")(other)


def test_the_context_shows_only_its_public_surface():
    ctx = JobContext("id", "name", 2)

    assert (ctx.id, ctx.name, ctx.attempts, ctx.cancelled.is_set(), ctx.time_left()) == ("id", "name", 2, False, None)
    assert not hasattr(ctx, "deadline")


def test_dispatch_async_sends_without_blocking_the_loop(aws, welcome, jobs_sent, monkeypatch):
    import ske_bridge.jobs.send

    threads = []

    class Recording:
        def send_message(self, **kwargs):
            threads.append(threading.current_thread() is threading.main_thread())
            jobs_sent.append(kwargs)

    monkeypatch.setattr(ske_bridge.jobs.send, "sqs_client", lambda: Recording())

    async def main():
        return await welcome.dispatch_async({"user_id": 7}, delay=5), await __import__("ske_bridge.jobs").jobs.dispatch_async("send-welcome-mail", 1)

    first, second = asyncio.run(main())

    assert threads == [False, False]
    assert json.loads(jobs_sent[0]["MessageBody"])["id"] == first and jobs_sent[0]["DelaySeconds"] == 5
    assert json.loads(jobs_sent[1]["MessageBody"])["id"] == second


def test_dispatch_async_raises_what_dispatch_raises(aws, jobs_sent):
    from ske_bridge.jobs import dispatch_async

    with pytest.raises(LookupError, match="no job named 'nowhere'"):
        asyncio.run(dispatch_async("nowhere"))


# Queue names, as Celery allows them


def test_on_aws_the_default_queue_is_allowed_even_when_not_listed(aws, welcome, jobs_sent, monkeypatch):
    monkeypatch.setenv("SQS_QUEUE", "main")
    monkeypatch.setenv("SQS_QUEUES", "emails")

    welcome.dispatch({})
    welcome.dispatch({}, queue="emails")

    assert [s["QueueUrl"].rsplit("/", 1)[1] for s in jobs_sent] == ["main--shop-production", "emails--shop-production"]


def test_on_aws_without_sqs_queues_only_the_default_queue_is_allowed(aws, welcome, jobs_sent):
    welcome.dispatch({})

    with pytest.raises(ValueError, match="no queue named 'emails'; its queues are default"):
        welcome.dispatch({}, queue="emails")
    assert len(jobs_sent) == 1


def test_on_gcp_without_ske_queues_no_queue_is_allowed(gcp, welcome, monkeypatch):
    monkeypatch.delenv("SKE_QUEUES")

    with pytest.raises(ValueError, match="no queue named 'default'; its queues are none"):
        welcome.dispatch({})
    assert gcp[0] == []


# Cloud Tasks errors and the token


def http_error(url, code, body):
    from urllib.error import HTTPError

    return HTTPError(url, code, {403: "Forbidden", 503: "Service Unavailable"}.get(code, "Error"), {}, io.BytesIO(body))


def test_on_gcp_a_refusal_carries_the_status_and_the_body(gcp, welcome, monkeypatch):
    import ske_bridge.cloudtasks as cloudtasks

    def refused(req, timeout):
        if req.full_url.startswith("http://metadata"):
            return io.BytesIO(b'{"access_token":"test-token","expires_in":3600}')
        raise http_error(req.full_url, 403, b'{"error":{"message":"The principal lacks cloudtasks.tasks.create"}}')

    monkeypatch.setattr(cloudtasks, "urlopen", refused)

    with pytest.raises(cloudtasks.CloudTasksError, match=r"refused the task for queue ske-shop-prod--default: HTTP 403 Forbidden: .*lacks cloudtasks\.tasks\.create") as raised:
        welcome.dispatch({})
    assert raised.value.status == 403
    assert not isinstance(raised.value, cloudtasks.CloudTasksUnavailable)


def test_on_gcp_an_unavailable_service_is_a_url_error(gcp, welcome, monkeypatch):
    from urllib.error import URLError

    import ske_bridge.cloudtasks as cloudtasks

    def unavailable(req, timeout):
        if req.full_url.startswith("http://metadata"):
            return io.BytesIO(b'{"access_token":"test-token","expires_in":3600}')
        raise http_error(req.full_url, 503, b"try later")

    monkeypatch.setattr(cloudtasks, "urlopen", unavailable)

    with pytest.raises(cloudtasks.CloudTasksUnavailable, match="HTTP 503 Service Unavailable: try later") as raised:
        welcome.dispatch({})
    assert isinstance(raised.value, URLError) and isinstance(raised.value, cloudtasks.CloudTasksError)


def test_a_token_the_metadata_server_refuses_carries_its_body(gcp, welcome, monkeypatch):
    import ske_bridge.cloudtasks as cloudtasks

    def refused(req, timeout):
        raise http_error(req.full_url, 403, b"no service account")

    monkeypatch.setattr(cloudtasks, "urlopen", refused)

    with pytest.raises(cloudtasks.CloudTasksError, match="metadata server refused the access token: HTTP 403 Forbidden: no service account"):
        welcome.dispatch({})


def test_the_token_is_kept_until_a_minute_before_it_expires(gcp, welcome, monkeypatch, cloud_tasks_token):
    sent, tokens = gcp
    now = [1_000_000.0]
    monkeypatch.setattr("ske_bridge.cloudtasks.time.time", lambda: now[0])

    welcome.dispatch({})
    assert cloud_tasks_token["expires_at"] == 1_000_000.0 + 3600 - 60
    now[0] += 3539
    welcome.dispatch({})
    now[0] += 1
    welcome.dispatch({})

    assert len(sent) == 3
    assert len(tokens) == 2


# Off the platform: one job at a time, by due time


def test_off_the_platform_jobs_run_one_at_a_time_in_due_order():
    order, running, overlaps = [], [], []
    done = threading.Event()

    @job("ordered")
    def ordered(payload, ctx):
        if running:
            overlaps.append(payload)
        running.append(payload)
        __import__("time").sleep(0.05)
        order.append(payload)
        running.remove(payload)
        if len(order) == 3:
            done.set()

    ordered.dispatch("late", delay=1)
    ordered.dispatch("first")
    ordered.dispatch("second")

    assert done.wait(5)
    assert order == ["first", "second", "late"]
    assert overlaps == []


def test_a_uuid_with_anything_after_it_is_refused():
    with pytest.raises(InvalidMessage, match='"id" is not a UUID'):
        decode(json.dumps({"job": "a", "payload": 1, "id": str(uuid.uuid4()) + "\n"}))
