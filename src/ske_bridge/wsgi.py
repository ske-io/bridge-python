"""The internal routes as WSGI middleware, for frameworks that are WSGI
applications underneath (Flask). It answers ``/__ske/*`` itself, answers web
requests while the platform has the app in maintenance (see
ske_bridge.maintenance), and passes everything else to the application with
the request id the logs carry (see ske_bridge.logging)."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from http import HTTPStatus
from typing import Any

from ske_bridge import maintenance
from ske_bridge.logging import request_id
from ske_bridge.routes import Routes, is_internal

WSGIApp = Callable[[dict[str, Any], Callable[..., Any]], Iterable[bytes]]


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
            decision = maintenance.check(path, environ.get("HTTP_COOKIE"), secure, is_internal(environ.get("HTTP_X_SKE_INTERNAL")))
            if isinstance(decision, maintenance.Bypass):
                start_response("302 Found", [("Location", "/"), ("Set-Cookie", decision.cookie), ("Content-Length", "0")])
                return [b""]
            if isinstance(decision, maintenance.Paused):
                page = self.maintenance_page(environ) or maintenance.DEFAULT_PAGE
                start_response("503 Service Unavailable", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(page)))])
                return [b"" if environ.get("REQUEST_METHOD") == "HEAD" else page]
            return self.app(environ, start_response)

        reply = self.routes.dispatch(
            environ.get("REQUEST_METHOD", "GET"),
            path,
            environ.get("HTTP_X_SKE_INTERNAL"),
            read_body(environ),
        )

        content = reply.content()
        start_response(
            f"{reply.status} {HTTPStatus(reply.status).phrase}",
            [("Content-Type", "application/json"), ("Content-Length", str(len(content)))],
        )
        return [content]


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
