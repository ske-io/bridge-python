"""The platform's middleware for Django: the internal routes (see
ske_bridge.routes), the client's address (see ske_bridge.forwarded),
maintenance mode (see ske_bridge.maintenance) and the request id the logs
carry (see ske_bridge.logging)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager

from django.http import HttpRequest, HttpResponse, HttpResponseRedirect

from ske_bridge import environment, maintenance
from ske_bridge.forwarded import client_address
from ske_bridge.logging import request_id
from ske_bridge.routes import INTERNAL_HEADER, Routes, is_internal


@contextmanager
def connection_upkeep() -> Iterator[None]:
    """Closes database connections that are broken or past ``CONN_MAX_AGE``
    around each task, as Celery's worker does for Django: one batch runs
    several tasks in one request, and a connection one of them broke must not
    fail the next."""
    from django.db import close_old_connections

    close_old_connections()
    try:
        yield
    finally:
        close_old_connections()


def _runner():
    from ske_bridge.celery import find_app
    from ske_bridge.celery.runner import QueueRunner

    return QueueRunner(find_app(), around_job=connection_upkeep)


routes = Routes(_runner)


def _maintenance_page(request: HttpRequest) -> bytes | str:
    """The project's ``503.html`` template, or the minimal page."""
    from django.template import TemplateDoesNotExist
    from django.template.loader import render_to_string

    try:
        return render_to_string(maintenance.TEMPLATE, request=request)
    except TemplateDoesNotExist:
        return maintenance.DEFAULT_PAGE


class InternalRoutesMiddleware:
    """Inert off the platform, even when listed in ``MIDDLEWARE`` by hand:
    there the forwarded headers are whatever a client sent."""

    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]):
        self.get_response = get_response
        self.active = environment.running_on_platform()

    def __call__(self, request: HttpRequest) -> HttpResponse:
        if not self.active:
            return self.get_response(request)

        token = request_id.set(request.headers.get("X-Request-Id") or None)
        try:
            return self._handle(request)
        finally:
            request_id.reset(token)

    def _handle(self, request: HttpRequest) -> HttpResponse:
        if not routes.handles(request.path_info):
            # The connection comes from the platform on loopback; the client
            # is the entry the platform appended to X-Forwarded-For.
            address = client_address(request.META.get("HTTP_X_FORWARDED_FOR"))
            if address is not None:
                request.META["REMOTE_ADDR"] = address

            decision = maintenance.check(
                request.path_info, request.META.get("HTTP_COOKIE"), request.is_secure(), is_internal(request.headers.get(INTERNAL_HEADER)),
            )
            if isinstance(decision, maintenance.Bypass):
                response = HttpResponseRedirect("/")
                response.headers["Set-Cookie"] = decision.cookie
                return response
            if isinstance(decision, maintenance.Paused):
                return HttpResponse(_maintenance_page(request), status=503)
            return self.get_response(request)

        # read() rather than request.body: the body property refuses anything
        # past DATA_UPLOAD_MAX_MEMORY_SIZE (2.5 MB), which a full batch of SQS
        # messages can exceed.
        reply = routes.dispatch(request.method or "GET", request.path_info, request.headers.get(INTERNAL_HEADER), request.read())
        return HttpResponse(reply.content(), status=reply.status, content_type="application/json")
