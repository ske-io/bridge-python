"""The internal routes as WSGI middleware, for frameworks that are WSGI
applications underneath (Flask) and for any other WSGI app through wrap(). It
answers ``/__ske/*`` itself, answers web requests while the platform has the
app in maintenance (see ske_bridge.maintenance), and passes everything else to
the application with the request id the logs carry (see ske_bridge.logging)."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from http import HTTPStatus
from typing import Any

from ske_bridge import environment, forwarded, generic, maintenance
from ske_bridge import logging as ske_logging
from ske_bridge.logging import request_id
from ske_bridge.routes import NOT_FOUND, Routes, is_internal

WSGIApp = Callable[[dict[str, Any], Callable[..., Any]], Iterable[bytes]]
MAINTENANCE_BYPASS_ENVIRON = "HTTP_" + maintenance.BYPASS_HEADER.upper().replace("-", "_")


class InternalRoutes:
    """``maintenance_page`` renders the app's own maintenance page for a
    request's environ, or returns None when it has none."""

    def __init__(self, app: WSGIApp, routes: Routes, maintenance_page: Callable[[dict[str, Any]], bytes | None] | None = None):
        self.app = app
        self.routes = routes
        self.maintenance_page = maintenance_page or (lambda environ: None)

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        token = request_id.set(environ.get("HTTP_X_REQUEST_ID") or None)
        try:
            return self._handle(environ, start_response)
        finally:
            request_id.reset(token)

    def _handle(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        path = environ.get("PATH_INFO") or "/"
        if not self.routes.handles(path):
            # On the platform X-Forwarded-Proto is set by the container's
            # entry process, never by the client; see ske_bridge.forwarded.
            secure = (environ.get("HTTP_X_FORWARDED_PROTO") or environ.get("wsgi.url_scheme")) == "https"
            decision = maintenance.check(
                path, environ.get("HTTP_COOKIE"), secure, is_internal(environ.get("HTTP_X_SKE_INTERNAL")),
                bypass_header=environ.get(MAINTENANCE_BYPASS_ENVIRON),
            )
            if isinstance(decision, maintenance.Bypass):
                start_response("302 Found", [("Location", "/"), ("Set-Cookie", decision.cookie), ("Content-Length", "0")])
                return [b""]
            if isinstance(decision, maintenance.Paused):
                page = self.maintenance_page(environ) or maintenance.DEFAULT_PAGE
                start_response("503 Service Unavailable", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(page)))])
                return [b"" if environ.get("REQUEST_METHOD") == "HEAD" else page]
            return self.app(environ, start_response)

        received = time.monotonic()
        reply = self.routes.dispatch(
            environ.get("REQUEST_METHOD", "GET"),
            path,
            environ.get("HTTP_X_SKE_INTERNAL"),
            read_body(environ),
            received,
        )

        content = reply.content()
        start_response(
            f"{reply.status} {HTTPStatus(reply.status).phrase}",
            [("Content-Type", "application/json"), ("Content-Length", str(len(content)))],
        )
        return [content]


class TrustedProxy:
    """Puts the client's address, scheme and host from the one trusted proxy
    hop where a WSGI app reads them (``REMOTE_ADDR``, ``wsgi.url_scheme``,
    ``HTTP_HOST``, ``SERVER_NAME``, ``SERVER_PORT``), as Werkzeug's ProxyFix
    does for Flask; see ske_bridge.forwarded. Only for the platform, where
    the headers are the platform's own."""

    def __init__(self, app: WSGIApp):
        self.app = app

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        origin = forwarded.origin(
            environ.get("HTTP_X_FORWARDED_FOR"),
            environ.get("HTTP_X_FORWARDED_PROTO"),
            environ.get("HTTP_X_FORWARDED_HOST"),
            environ.get("HTTP_X_FORWARDED_PORT"),
            environ.get("wsgi.url_scheme") or "http",
        )
        if origin.client is not None:
            environ["REMOTE_ADDR"] = origin.client
        if origin.scheme is not None:
            environ["wsgi.url_scheme"] = origin.scheme
        if origin.host is not None:
            environ["HTTP_HOST"] = origin.host
        if origin.server is not None:
            environ["SERVER_NAME"], environ["SERVER_PORT"] = origin.server[0], str(origin.server[1])
        return self.app(environ, start_response)


def wrap(app: WSGIApp | str) -> WSGIApp:
    """Any WSGI app, or the ``module:attribute`` import string naming one,
    with the platform's middleware: the internal routes, maintenance mode and
    the request id (InternalRoutes), and the trusted hop's client, scheme and
    host (TrustedProxy). Structured logs are set up too.

    Off the platform the app is returned unchanged, and so is an app already
    wrapped. An app that has the bridge's Django or Flask integration is
    refused. A server that loads its app from an import string can call it
    there, for gunicorn ``wsgi_app = 'ske_bridge.wsgi:wrap("main:app")'``.
    See ske_bridge.generic."""
    if isinstance(app, str):
        app = generic.load(app)
    generic.refuse_integrated(app)
    if not environment.running_on_platform() or isinstance(app, InternalRoutes):
        return app

    ske_logging.configure()
    return InternalRoutes(TrustedProxy(app), Routes(generic.queue_runner))


def queue(target: str) -> WSGIApp:
    """A WSGI app that serves only the internal routes for the app the
    ``module:attribute`` import string names, for a process that runs
    delivered jobs and serves no web traffic: every other path is a 404.

    The app is imported, so what its import sets up (its Celery app, its
    jobs) is there, but never called, so it may be WSGI or ASGI. Served by a
    sync WSGI worker, which runs requests on the main thread, the jobs get
    Celery's time limits and the batch's deadline, which an ASGI app's event
    loop cannot give them. An app that has the bridge's Django or Flask
    integration is refused, as wrap() refuses it. For gunicorn
    ``wsgi_app = 'ske_bridge.wsgi:queue("main:app")'``."""
    generic.refuse_integrated(generic.load(target))
    ske_logging.configure()
    return InternalRoutes(TrustedProxy(_not_found), Routes(generic.queue_runner))


def _not_found(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
    content = NOT_FOUND.content()
    start_response("404 Not Found", [("Content-Type", "application/json"), ("Content-Length", str(len(content)))])
    return [b"" if environ.get("REQUEST_METHOD") == "HEAD" else content]


def read_body(environ: dict[str, Any]) -> bytes:
    """The request body, all of it. A queue batch is up to ten SQS messages of
    256 KB each, so no framework's form-size limit applies here."""
    stream = environ.get("wsgi.input")
    if stream is None:
        return b""
    try:
        length = int(environ.get("CONTENT_LENGTH") or -1)
    except ValueError:
        length = -1
    return stream.read(length) if length >= 0 else stream.read()
