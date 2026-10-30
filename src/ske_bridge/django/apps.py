from __future__ import annotations

from typing import Any

from django.apps import AppConfig

from ske_bridge import environment
from ske_bridge.mail import mail_service

MIDDLEWARE = "ske_bridge.django.middleware.InternalRoutesMiddleware"


class SkeBridgeConfig(AppConfig):
    name = "ske_bridge.django"
    label = "ske_bridge"
    verbose_name = "SKE"

    def ready(self) -> None:
        if environment.running_on_platform():
            from django.conf import settings

            install_middleware(settings)
            trust_forwarded_proto(settings)
            use_mail_service(settings)

            from ske_bridge.logging import configure

            # Django has applied LOGGING by the time apps are ready.
            configure()


SMTP_BACKEND = "django.core.mail.backends.smtp.EmailBackend"


def use_mail_service(settings: Any) -> None:
    """Points Django's SMTP backend at the platform's mail service when one is
    attached (see ske_bridge.mail): its host and port, no TLS or SSL, and no
    login. This overrides the project's own ``EMAIL_HOST`` settings. A project
    that sends through another backend (console, locmem, a provider's API) is
    left alone, since it does not send over SMTP."""
    service = mail_service()
    if service is None or getattr(settings, "EMAIL_BACKEND", SMTP_BACKEND) != SMTP_BACKEND:
        return

    settings.EMAIL_HOST, settings.EMAIL_PORT = service
    settings.EMAIL_USE_TLS = False
    settings.EMAIL_USE_SSL = False
    settings.EMAIL_HOST_USER = ""
    settings.EMAIL_HOST_PASSWORD = ""


def trust_forwarded_proto(settings: Any) -> None:
    """Sets ``SECURE_PROXY_SSL_HEADER`` so ``request.is_secure()`` reports the
    scheme the client used, unless the project set it. It is safe on the
    platform: the container's entry process replaces ``X-Forwarded-Proto`` on
    every request, so a client cannot choose it (see ske_bridge.forwarded)."""
    if not getattr(settings, "SECURE_PROXY_SSL_HEADER", None):
        settings.SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")


def install_middleware(settings: Any) -> None:
    """Puts the internal routes' middleware first in ``MIDDLEWARE``, so the
    application's urls.py needs no change and no other middleware (host
    validation, CSRF, authentication) sees the platform's requests.

    App configs are ready before the WSGI handler, and the test client's,
    loads its middleware, so the change is in place for the first request.
    A project that lists the middleware itself keeps its position.
    """
    middleware = list(settings.MIDDLEWARE)
    if MIDDLEWARE not in middleware:
        settings.MIDDLEWARE = [MIDDLEWARE, *middleware]
