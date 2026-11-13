"""What the bridge does for an application that is neither Django nor Flask:
any WSGI or ASGI app (FastAPI, Starlette, Litestar, Falcon, Bottle, …).

ske_bridge.wsgi.wrap and ske_bridge.asgi.wrap take the app, or an import
string naming it, and return it wrapped with the platform's middleware on the
platform, unchanged off it. A server that loads its app from an import string
can call them in that string, as gunicorn does::

    # gunicorn.conf.py
    wsgi_app = 'ske_bridge.wsgi:wrap("main:app")'

Such an app has no framework hooks to plug into, so the bridge does only what
needs none: the internal routes, maintenance mode with the default page, the
client's address, scheme and host from the trusted hop, and structured logs.
"""

from __future__ import annotations

import importlib
import sys
from typing import Any

from ske_bridge import environment
from ske_bridge.routes import Runner


class AppImportError(ImportError):
    """The import string names no app that can be imported."""


class NoQueueRunner(LookupError):
    """Nothing in this app runs queued jobs."""


class AlreadyIntegrated(TypeError):
    """The app has the bridge's framework integration, which wrapping would
    shadow."""


#: The Django integration's middleware, as ske_bridge.django.apps names it.
DJANGO_MIDDLEWARE = "ske_bridge.django.middleware.InternalRoutesMiddleware"


def load(target: str) -> Any:
    """The object a ``module:attribute`` import string names; the attribute
    may be dotted (``main:api.app``)."""
    module_name, sep, attribute = target.partition(":")
    if not sep or not module_name.strip() or not attribute.strip():
        raise AppImportError(f"{target!r} is not an import string: name the app as module:attribute, for example main:app")

    module_name, attribute = module_name.strip(), attribute.strip()
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise AppImportError(f"cannot import the app {target!r}: {type(e).__name__}: {e}") from e

    app: Any = module
    for name in attribute.split("."):
        try:
            app = getattr(app, name)
        except AttributeError:
            raise AppImportError(f"cannot import the app {target!r}: module {module_name!r} has no attribute {attribute!r}") from None
    return app


def queue_runner() -> Runner:
    """What runs the jobs ``/__ske/queue`` delivers: the Celery app
    ``SKE_CELERY_APP`` or ``CELERY_APP`` names, as ``celery -A`` takes it.

    With neither set it raises NoQueueRunner, which the queue route answers
    with a 500 that names it, so the platform delivers the batch again once
    the deployment is fixed and its dead-letter queue keeps what never runs:
    the same answer as for a Celery app that cannot be found."""
    if environment.get("SKE_CELERY_APP") or environment.get("CELERY_APP"):
        from ske_bridge.celery import find_app
        from ske_bridge.celery.runner import QueueRunner

        return QueueRunner(find_app())

    raise NoQueueRunner(
        "nothing runs queued jobs in this app: set SKE_CELERY_APP to its Celery app "
        "as celery -A names it (for example proj.celery:app)"
    )


def refuse_integrated(app: Any) -> None:
    """Raises AlreadyIntegrated for an app that has the bridge's Flask
    extension or its Django app: wrapping it would answer the platform's
    routes without what the integration adds (the Celery app it found, an
    application context or database upkeep around each task, the app's
    ``503.html``)."""
    from ske_bridge.wsgi import InternalRoutes

    extensions = getattr(app, "extensions", None)
    if (isinstance(extensions, dict) and "ske" in extensions) or isinstance(getattr(app, "wsgi_app", None), InternalRoutes):
        raise AlreadyIntegrated("this Flask app has SkeBridge already: serve it as it is, without ske_bridge's wrap()")

    # Only an app that is a Django handler can need Django's settings, so
    # Django is never imported here.
    handlers = sys.modules.get("django.core.handlers.base")
    if handlers is not None and isinstance(app, handlers.BaseHandler):
        from django.conf import settings

        if "ske_bridge.django" in settings.INSTALLED_APPS or DJANGO_MIDDLEWARE in settings.MIDDLEWARE:
            raise AlreadyIntegrated("this Django app has ske_bridge.django already: serve it as it is, without ske_bridge's wrap()")
