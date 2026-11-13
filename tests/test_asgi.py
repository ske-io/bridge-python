import asyncio
import json
import threading

import pytest
from celery import shared_task

import ske_bridge.logging
from ske_bridge import asgi
from ske_bridge.logging import request_id
from ske_bridge.maintenance import DEFAULT_PAGE
from tests.conftest import envelope, record


class App:
    """An ASGI app that answers ``ok`` and keeps the scope and request id
    each call saw."""

    def __init__(self):
        self.calls = []

    async def __call__(self, scope, receive, send):
        self.calls.append((scope, request_id.get()))
        if scope["type"] == "lifespan":
            message = await receive()
            await send({"type": message["type"] + ".complete"})
        elif scope["type"] == "websocket":
            await receive()
            await send({"type": "websocket.accept"})
        else:
            await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"ok"})


app = App()


def scope(path="/", method="GET", headers=None, kind="http", **extra):
    return {
        "type": kind,
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "ws" if kind == "websocket" else "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()],
        "client": ("127.0.0.1", 50000),
        "server": ("127.0.0.1", 8000),
        **extra,
    }


def call(target, scope, messages=None):
    """Runs one ASGI call and returns what the app sent."""
    return asyncio.run(acall(target, scope, messages))


async def acall(target, scope, messages=None):
    sent = []
    inbox = list(messages if messages is not None else [{"type": "http.request", "body": b"", "more_body": False}])

    async def receive():
        return inbox.pop(0) if inbox else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    await target(scope, receive, send)
    return sent


def response(sent):
    start, body = sent
    assert start["type"] == "http.response.start"
    return start["status"], {k.decode(): v.decode() for k, v in start["headers"]}, body["body"]


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
    return asgi.wrap(downstream)


def queue_call(wrapped, records, internal=True, chunks=1):
    """The queue request for a batch, as a coroutine to await."""
    body = json.dumps({"source": "sqs", "records": records}).encode()
    size = len(body) // chunks + 1
    parts = [body[i:i + size] for i in range(0, len(body), size)]
    messages = [{"type": "http.request", "body": p, "more_body": i < len(parts) - 1} for i, p in enumerate(parts)]
    headers = {"X-Ske-Internal": "true"} if internal else {}
    return acall(wrapped, scope("/__ske/queue", "POST", headers), messages)


def queue(wrapped, records, internal=True, chunks=1):
    return response(asyncio.run(queue_call(wrapped, records, internal, chunks)))


def test_the_queue_runs_celery_tasks_from_a_body_in_chunks(wrapped, downstream, publish):
    from tests.celery_app import calls

    status, headers, body = queue(wrapped, [record(publish("tests.add", (2, 3)))], chunks=3)

    assert (status, headers["content-type"]) == (200, "application/json")
    assert json.loads(body)["outcomes"][0]["deleted"]
    assert calls == [("add", 2, 3)]
    assert downstream.calls == []


def test_the_queue_needs_the_internal_header(wrapped, publish):
    status, _, body = queue(wrapped, [record(publish("tests.add", (1, 1)))], internal=False)

    assert (status, json.loads(body)) == (404, {"error": "not found"})


def test_without_a_celery_app_the_queue_answers_500_naming_what_is_missing(wrapped, monkeypatch):
    monkeypatch.delenv("SKE_CELERY_APP")

    status, _, body = queue(wrapped, [record("{}")])

    assert status == 500
    assert json.loads(body)["error"].startswith("NoQueueRunner: nothing runs queued jobs in this app: set SKE_CELERY_APP")


@shared_task(name="tests.shared_child")
def shared_child(value):
    from tests.celery_app import calls

    calls.append(("child", value))


def test_a_task_can_send_a_shared_task_from_the_queue_thread(wrapped, celery_app, publish, sqs_sent):
    @celery_app.task(name="tests.parent")
    def parent(value):
        shared_child.delay(value)

    status, _, body = queue(wrapped, [record(publish("tests.parent", (7,)))])

    outcome = json.loads(body)["outcomes"][0]
    assert (status, outcome["deleted"], outcome["failed"]) == (200, True, False), outcome
    assert envelope(sqs_sent[-1]["MessageBody"])["headers"]["task"] == "tests.shared_child"


def test_concurrent_batches_keep_their_own_task_state(wrapped, celery_app, publish):
    barrier = threading.Barrier(2, timeout=5)
    runs = {}

    @celery_app.task(name="tests.together")
    def together(name, fail):
        # Both batches' tasks are running now; each must see its own run.
        barrier.wait()
        runs[name] = wrapped.routes._runner.celery._run
        barrier.wait()
        if fail:
            raise ValueError(name)

    async def both():
        return await asyncio.gather(
            queue_call(wrapped, [record(publish("tests.together", ("a", False)), message_id="a")]),
            queue_call(wrapped, [record(publish("tests.together", ("b", True)), message_id="b")]),
        )

    first, second = (json.loads(response(sent)[2])["outcomes"][0] for sent in asyncio.run(both()))

    assert (first["message_id"], first["deleted"], first["failed"]) == ("a", True, False)
    assert (second["message_id"], second["failed"], second["error"]) == ("b", True, "ValueError: b")
    assert runs["a"] is not None and runs["b"] is not None and runs["a"] is not runs["b"]


def test_the_app_and_readiness_are_served_while_a_batch_runs(wrapped, downstream, celery_app, publish):
    started, release = threading.Event(), threading.Event()

    @celery_app.task(name="tests.held")
    def held():
        started.set()
        release.wait(5)

    async def meanwhile():
        batch = asyncio.ensure_future(queue_call(wrapped, [record(publish("tests.held"))]))
        assert await asyncio.to_thread(started.wait, 5)
        health = await acall(wrapped, scope("/__ske/health"))
        page = await acall(wrapped, scope("/users"))
        running = not batch.done()
        release.set()
        return running, health, page, await batch

    running, health, page, batch = asyncio.run(meanwhile())

    assert running
    assert response(health)[0] == 200
    assert response(page)[::2] == (200, b"ok")
    assert response(batch)[0] == 200


def test_a_queue_request_cut_off_mid_body_runs_nothing_and_answers_no_one(wrapped, monkeypatch):
    dispatched = []
    monkeypatch.setattr(wrapped.routes, "dispatch", lambda *args: dispatched.append(args))
    messages = [{"type": "http.request", "body": b'{"records": [', "more_body": True}, {"type": "http.disconnect"}]

    sent = call(wrapped, scope("/__ske/queue", "POST", {"X-Ske-Internal": "true"}), messages)

    assert (sent, dispatched) == ([], [])


def test_health_answers_for_readiness(wrapped, downstream):
    status, headers, body = response(call(wrapped, scope("/__ske/health")))

    assert (status, json.loads(body)) == (200, {"status": "ok"})
    assert headers["content-length"] == str(len(body))
    assert downstream.calls == []


def test_a_head_request_gets_no_body(wrapped):
    status, headers, body = response(call(wrapped, scope("/__ske/health", "HEAD")))

    assert (status, body) == (200, b"")
    assert headers["content-length"] == str(len(b'{"status": "ok"}'))


def test_other_paths_reach_the_application(wrapped, downstream):
    status, _, body = response(call(wrapped, scope("/users")))

    assert (status, body) == (200, b"ok")
    assert downstream.calls[0][0]["path"] == "/users"


def test_the_request_id_is_set_around_the_call(wrapped, downstream):
    call(wrapped, scope("/", headers={"X-Request-Id": "req-1"}))

    assert downstream.calls[0][1] == "req-1"
    assert request_id.get() is None


def test_the_request_id_reaches_the_queue_runner(wrapped, monkeypatch):
    seen = []
    monkeypatch.setattr(wrapped.routes, "dispatch", lambda *args: seen.append(request_id.get()) or asgi.NOT_FOUND)

    call(wrapped, scope("/__ske/queue", "POST", {"X-Request-Id": "req-2"}))

    assert seen == ["req-2"]


def test_lifespan_reaches_the_application_untouched(wrapped, downstream):
    lifespan = {"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}

    sent = call(wrapped, lifespan, [{"type": "lifespan.startup"}])

    assert sent == [{"type": "lifespan.startup.complete"}]
    assert downstream.calls[0][0] is lifespan


def test_a_websocket_reaches_the_application(wrapped, downstream):
    sent = call(wrapped, scope("/ws", kind="websocket"), [{"type": "websocket.connect"}])

    assert sent == [{"type": "websocket.accept"}]


def test_a_websocket_to_the_internal_routes_is_refused(wrapped, downstream):
    sent = call(wrapped, scope("/__ske/health", kind="websocket"), [{"type": "websocket.connect"}])

    assert sent == [{"type": "websocket.close", "code": 1008}]
    assert downstream.calls == []


def test_a_websocket_to_the_internal_routes_gets_404_when_the_server_can_answer_one(wrapped):
    ws = scope("/__ske/queue", kind="websocket", extensions={"websocket.http.response": {}})

    start, body = call(wrapped, ws, [{"type": "websocket.connect"}])

    assert (start["type"], start["status"]) == ("websocket.http.response.start", 404)
    assert json.loads(body["body"]) == {"error": "not found"}


def test_off_the_platform_the_app_is_returned_unchanged(downstream, logs_configured):
    assert asgi.wrap(downstream) is downstream
    assert logs_configured == []


def test_on_the_platform_logs_are_structured(wrapped, logs_configured):
    assert logs_configured == [True]


def test_an_app_is_wrapped_once(wrapped):
    assert asgi.wrap(wrapped) is wrapped


def test_the_app_comes_from_an_import_string(platform):
    wrapped = asgi.wrap("tests.test_asgi:app")

    assert isinstance(wrapped, asgi.InternalRoutes)
    assert wrapped.app.app is app


def test_off_the_platform_the_import_string_gives_the_app_itself():
    assert asgi.wrap("tests.test_asgi:app") is app


# Maintenance mode


@pytest.fixture
def maintenance(platform, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    monkeypatch.setenv("MAINTENANCE_MODE_SECRET", "let-me-in")
    monkeypatch.setenv("SKE_MAINTENANCE_BYPASS_TOKEN", "platform-token")


def test_maintenance_answers_503_with_the_default_page(maintenance, wrapped, downstream):
    status, headers, body = response(call(wrapped, scope("/")))

    assert (status, body) == (503, DEFAULT_PAGE)
    assert headers["content-type"] == "text/html; charset=utf-8"
    assert downstream.calls == []


def test_maintenance_answers_head_without_a_body(maintenance, wrapped):
    assert response(call(wrapped, scope("/", "HEAD")))[::2] == (503, b"")


def test_maintenance_keeps_the_internal_routes(maintenance, wrapped):
    assert response(call(wrapped, scope("/__ske/health")))[0] == 200


def test_maintenance_lets_the_platforms_requests_in(maintenance, wrapped):
    assert response(call(wrapped, scope("/", headers={"X-Ske-Internal": "true"})))[0] == 200


def test_maintenance_lets_the_platforms_bypass_header_in(maintenance, wrapped):
    assert response(call(wrapped, scope("/", headers={"SKE-Maintenance-Bypass": "platform-token"})))[0] == 200
    assert response(call(wrapped, scope("/", headers={"SKE-Maintenance-Bypass": "guess"})))[0] == 503


def test_the_secret_sets_a_cookie_that_lets_the_browser_in(maintenance, wrapped):
    status, headers, _ = response(call(wrapped, scope("/let-me-in", headers={"X-Forwarded-Proto": "https"})))

    assert (status, headers["location"]) == (302, "/")
    assert "Secure" in headers["set-cookie"]
    cookie = headers["set-cookie"].split(";", 1)[0]
    assert response(call(wrapped, scope("/", headers={"Cookie": f"theme=dark; {cookie}"})))[0] == 200


def test_maintenance_refuses_a_websocket_with_a_close(maintenance, wrapped, downstream):
    sent = call(wrapped, scope("/ws", kind="websocket"), [{"type": "websocket.connect"}])

    assert sent == [{"type": "websocket.close", "code": 1013}]
    assert downstream.calls == []


def test_maintenance_refuses_a_websocket_with_503_when_the_server_can_answer_one(maintenance, wrapped):
    ws = scope("/ws", kind="websocket", extensions={"websocket.http.response": {}})

    start, body = call(wrapped, ws, [{"type": "websocket.connect"}])

    assert (start["type"], start["status"]) == ("websocket.http.response.start", 503)
    assert body == {"type": "websocket.http.response.body", "body": DEFAULT_PAGE}


def test_maintenance_lets_a_websocket_with_the_bypass_header_in(maintenance, wrapped):
    ws = scope("/ws", kind="websocket", headers={"SKE-Maintenance-Bypass": "platform-token"})

    assert call(wrapped, ws, [{"type": "websocket.connect"}]) == [{"type": "websocket.accept"}]


# The trusted hop

FORWARDED = {
    "Host": "127.0.0.1:8000",
    "X-Forwarded-For": "203.0.113.9, 198.51.100.7",
    "X-Forwarded-Proto": "https",
    "X-Forwarded-Host": "app.example.com",
    "X-Forwarded-Port": "443",
}


def seen(wrapped, downstream, headers, kind="http"):
    messages = [{"type": "websocket.connect"}] if kind == "websocket" else None
    call(wrapped, scope("/", headers=headers, kind=kind), messages)
    seen = downstream.calls[-1][0]
    return seen["client"], seen["scheme"], asgi.header(seen, "host"), seen["server"]


def test_the_client_scheme_and_host_are_the_trusted_hops(wrapped, downstream):
    assert seen(wrapped, downstream, FORWARDED) == (("198.51.100.7", 0), "https", "app.example.com", ("app.example.com", 443))


def test_a_websocket_gets_the_trusted_hops_secure_scheme(wrapped, downstream):
    assert seen(wrapped, downstream, FORWARDED, kind="websocket")[1] == "wss"


def test_a_non_default_port_stays_in_the_host(wrapped, downstream):
    headers = {**FORWARDED, "X-Forwarded-Port": "8443"}

    assert seen(wrapped, downstream, headers)[2:] == ("app.example.com:8443", ("app.example.com", 8443))


def test_an_ipv6_host_has_no_brackets_in_server(wrapped, downstream):
    headers = {**FORWARDED, "X-Forwarded-Host": "[2001:db8::1]"}

    assert seen(wrapped, downstream, headers)[2:] == ("[2001:db8::1]", ("2001:db8::1", 443))


def test_a_forwarded_port_without_a_forwarded_host_leaves_the_host(wrapped, downstream):
    headers = {"Host": "127.0.0.1:8000", "X-Forwarded-Port": "443"}

    assert seen(wrapped, downstream, headers)[2:] == ("127.0.0.1:8000", ("127.0.0.1", 8000))


def test_without_forwarded_headers_nothing_changes(wrapped, downstream):
    assert seen(wrapped, downstream, {"Host": "127.0.0.1:8000"}) == (("127.0.0.1", 50000), "http", "127.0.0.1:8000", ("127.0.0.1", 8000))


def test_a_forwarded_for_entry_that_is_not_an_address_is_ignored(wrapped, downstream):
    assert seen(wrapped, downstream, {"X-Forwarded-For": "unknown"})[0] == ("127.0.0.1", 50000)


def test_the_servers_scope_is_left_as_it_is(wrapped):
    original = scope("/", headers=FORWARDED)

    call(wrapped, original)

    assert original["client"] == ("127.0.0.1", 50000)
    assert asgi.header(original, "host") == "127.0.0.1:8000"
