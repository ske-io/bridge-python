"""The platform's middleware for ASGI apps (FastAPI, Starlette, Litestar, …),
the counterpart of ske_bridge.wsgi. wrap() applies it on the platform.

``InternalRoutes`` answers ``/__ske/*`` itself (see ske_bridge.routes),
answers web requests while the platform has the app in maintenance (see
ske_bridge.maintenance) and passes everything else to the application with
the request id the logs carry (see ske_bridge.logging). ``TrustedProxy``
puts the client's address, scheme and host from the one trusted hop into the
scope (see ske_bridge.forwarded).

Lifespan events, and any scope type other than ``http`` and ``websocket``,
reach the application untouched.

A WebSocket handshake is web traffic like any other request, so maintenance
mode refuses it unless it is let in the same ways (the platform's internal
header, its bypass header, the bypass cookie): an app in maintenance takes no
new connections, as it takes no requests. The refusal is a 503 with the
maintenance page when the server supports the ``websocket.http.response``
extension, and otherwise a close before the handshake is accepted, which the
server answers with a 403. Connections opened before maintenance began are
the application's own; the platform's redeploy ends them. A WebSocket to
``/__ske/*`` is refused the same way with a 404: the platform never opens
one, and those paths are not the application's.
"""

from __future__ import annotations

import asyncio
import contextvars
import time
from collections.abc import Awaitable, Callable, MutableMapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ske_bridge import environment, forwarded, generic, maintenance
from ske_bridge import logging as ske_logging
from ske_bridge.logging import request_id
from ske_bridge.routes import INTERNAL_HEADER, NOT_FOUND, QUEUE_PATH, Routes, is_internal

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

#: The WebSocket close code for "try again later".
TRY_AGAIN_LATER = 1013


def header(scope: Scope, name: str) -> str | None:
    """One request header from an ASGI scope, its repeats joined as HTTP
    joins them (``; `` for Cookie, ``, `` for the rest), or None."""
    key = name.lower().encode("latin-1")
    values = [value.decode("latin-1") for field, value in scope.get("headers") or () if field.lower() == key]
    if not values:
        return None
    return ("; " if key == b"cookie" else ", ").join(values)


class InternalRoutes:
    """A delivered batch runs in a thread of the middleware's own pool: its
    jobs block for as long as they run, and the event loop, and the default
    pool the app's async libraries use, must keep serving meanwhile."""

    def __init__(self, app: ASGIApp, routes: Routes):
        self.app = app
        self.routes = routes
        self._queue_pool: ThreadPoolExecutor | None = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        kind = scope.get("type")
        if kind not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        token = request_id.set(header(scope, "x-request-id") or None)
        try:
            if kind == "http":
                await self._http(scope, receive, send)
            else:
                await self._websocket(scope, receive, send)
        finally:
            request_id.reset(token)

    async def _http(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path") or "/"
        method = scope.get("method") or "GET"
        if not self.routes.handles(path):
            decision = _maintenance(scope, path)
            if isinstance(decision, maintenance.Bypass):
                await _respond(send, 302, [(b"location", b"/"), (b"set-cookie", decision.cookie.encode("latin-1"))], b"")
            elif isinstance(decision, maintenance.Paused):
                page = maintenance.DEFAULT_PAGE
                await _respond(send, 503, [(b"content-type", b"text/html; charset=utf-8")], page, head=method == "HEAD")
            else:
                await self.app(scope, receive, send)
            return

        internal = header(scope, INTERNAL_HEADER)
        if path == QUEUE_PATH and method == "POST" and is_internal(internal):
            received = time.monotonic()
            body = await _read_body(receive)
            if body is None:
                # The platform gave up on the request: running the batch
                # would answer no one, and it delivers the batch again.
                return
            reply = await self._in_queue_pool(self.routes.dispatch, method, path, internal, body, received)
        else:
            # Health, 404 and 405 answer at once, never behind a batch.
            reply = self.routes.dispatch(method, path, internal, b"")
        await _respond(send, reply.status, [(b"content-type", b"application/json")], reply.content(), head=method == "HEAD")

    async def _in_queue_pool(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Runs fn in the queue pool with this context, the request id in it."""
        if self._queue_pool is None:
            self._queue_pool = ThreadPoolExecutor(thread_name_prefix="ske-queue")
        context = contextvars.copy_context()
        return await asyncio.get_running_loop().run_in_executor(self._queue_pool, context.run, fn, *args)

    async def _websocket(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path") or "/"
        if self.routes.handles(path):
            await _refuse(scope, receive, send, NOT_FOUND.status, b"application/json", NOT_FOUND.content())
        elif _maintenance(scope, path) is not None:
            await _refuse(scope, receive, send, 503, b"text/html; charset=utf-8", maintenance.DEFAULT_PAGE)
        else:
            await self.app(scope, receive, send)


class TrustedProxy:
    """Puts the client's address, scheme and host from the one trusted proxy
    hop into the scope (``client``, ``scheme``, the ``host`` header and
    ``server``), for HTTP and WebSocket alike; see ske_bridge.forwarded. Only
    for the platform, where the headers are the platform's own. The
    application gets a copy of the scope; the server's is left as it is."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") in ("http", "websocket"):
            scope = _forwarded(scope)
        await self.app(scope, receive, send)


def wrap(app: ASGIApp | str) -> ASGIApp:
    """Any ASGI app, or the ``module:attribute`` import string naming one,
    with the platform's middleware (InternalRoutes over TrustedProxy).
    Structured logs are set up too.

    Off the platform the app is returned unchanged, and so is an app already
    wrapped. An app that has the bridge's Django or Flask integration is
    refused. See ske_bridge.generic."""
    if isinstance(app, str):
        app = generic.load(app)
    generic.refuse_integrated(app)
    if not environment.running_on_platform() or isinstance(app, InternalRoutes):
        return app

    ske_logging.configure()
    return InternalRoutes(TrustedProxy(app), Routes(generic.queue_runner))


def _maintenance(scope: Scope, path: str) -> maintenance.Bypass | maintenance.Paused | None:
    # On the platform X-Forwarded-Proto is set by the container's entry
    # process, never by the client; see ske_bridge.forwarded.
    proto = header(scope, "x-forwarded-proto")
    secure = proto == "https" if proto else scope.get("scheme") in ("https", "wss")
    return maintenance.check(
        path, header(scope, "cookie"), secure, is_internal(header(scope, INTERNAL_HEADER)),
        bypass_header=header(scope, maintenance.BYPASS_HEADER),
    )


def _forwarded(scope: Scope) -> Scope:
    websocket = scope["type"] == "websocket"
    own = scope.get("scheme") or ("ws" if websocket else "http")
    origin = forwarded.origin(
        header(scope, "x-forwarded-for"),
        header(scope, "x-forwarded-proto"),
        header(scope, "x-forwarded-host"),
        header(scope, "x-forwarded-port"),
        {"ws": "http", "wss": "https"}.get(own, own),
    )

    scope = dict(scope)
    if origin.client is not None:
        scope["client"] = (origin.client, 0)
    if origin.scheme is not None:
        scope["scheme"] = {"http": "ws", "https": "wss"}[origin.scheme] if websocket else origin.scheme
    if origin.host is not None:
        host = origin.host.encode("latin-1")
        scope["headers"] = [(k, v) for k, v in scope.get("headers") or () if k.lower() != b"host"] + [(b"host", host)]
    if origin.server is not None:
        scope["server"] = origin.server
    return scope


async def _read_body(receive: Receive) -> bytes | None:
    """The request body, all of it (see ske_bridge.wsgi.read_body), or None
    when the client disconnects before sending it all."""
    chunks = []
    while True:
        message = await receive()
        if message.get("type") != "http.request":
            return None
        chunks.append(message.get("body") or b"")
        if not message.get("more_body"):
            return b"".join(chunks)


async def _respond(send: Send, status: int, headers: list[tuple[bytes, bytes]], body: bytes, head: bool = False) -> None:
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [*headers, (b"content-length", str(len(body)).encode())],
    })
    await send({"type": "http.response.body", "body": b"" if head else body})


async def _refuse(scope: Scope, receive: Receive, send: Send, status: int, content_type: bytes, body: bytes) -> None:
    """Refuses a WebSocket handshake: with an HTTP response when the server
    can send one, else by closing it before it is accepted."""
    message = await receive()
    if message.get("type") != "websocket.connect":
        return
    if "websocket.http.response" in (scope.get("extensions") or {}):
        await send({
            "type": "websocket.http.response.start",
            "status": status,
            "headers": [(b"content-type", content_type), (b"content-length", str(len(body)).encode())],
        })
        await send({"type": "websocket.http.response.body", "body": body})
    else:
        await send({"type": "websocket.close", "code": TRY_AGAIN_LATER if status == 503 else 1008})

