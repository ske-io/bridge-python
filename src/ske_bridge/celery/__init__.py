"""Celery on the platform: finding the application's Celery app, and the
settings that point it at the platform's queue.

Celery is an optional dependency (``ske-bridge[celery]``) and is imported only
when a queue delivery or a scheduler run needs it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ske_bridge import environment

if TYPE_CHECKING:
    from celery import Celery

_prepared: set[int] = set()


class NoCeleryApp(LookupError):
    """No Celery app is configured for this process."""


def find_app(explicit: Any = None, fallback: Any = None) -> Celery:
    """The application's Celery app.

    In order: ``explicit`` (``SkeBridge(app, celery=...)``); then the app
    ``SKE_CELERY_APP`` names, then ``CELERY_APP`` (the variable the ``celery``
    command reads for ``-A``), both named as ``-A`` takes it (``proj``,
    ``proj.celery``, ``proj.celery:app``); then ``fallback``, the app the
    framework integration found (a Flask app's ``extensions["celery"]``); then
    the app the project made current by creating it, as Django's
    ``proj/__init__.py`` convention does on import.

    With none of these it raises NoCeleryApp rather than hand back Celery's
    built-in default app, which has none of the project's tasks and would
    fail every message as unregistered.
    """
    from celery import _state
    from celery.app.utils import find_app as find_named_app

    name = environment.get("SKE_CELERY_APP") or environment.get("CELERY_APP")
    if explicit is not None:
        app = explicit
    elif name is not None:
        app = find_named_app(name)
    elif fallback is not None:
        app = fallback
    else:
        app = getattr(_state._tls, "current_app", None) or _only_app(_state)

    prepare(app)
    return app


def _only_app(state: Any) -> Celery:
    """The one app the process created, whichever thread created it (the
    current app is per thread, and gunicorn's threaded workers serve
    requests off the thread that imported the project). Celery's built-in
    default app does not count."""
    apps = [a for a in state._get_active_apps() if a is not state.default_app]
    if len(apps) == 1:
        return apps[0]
    if apps:
        raise NoCeleryApp(f"{len(apps)} Celery apps exist; set SKE_CELERY_APP to the one that runs tasks (for example proj.celery:app)")
    raise NoCeleryApp(
        "no Celery app is configured: set SKE_CELERY_APP to the app as celery -A names it "
        "(for example proj.celery:app), or import it where the project starts"
    )


def prepare(app: Celery) -> None:
    """Imports the app's task modules and finalizes it, as a worker does when
    it starts: ``include``, ``imports`` and ``autodiscover_tasks()`` all wait
    for this. Periodic tasks added with ``add_periodic_task`` in an
    ``on_after_configure`` or ``on_after_finalize`` handler are in the
    schedule once it has run. Once per app per process."""
    if id(app) in _prepared:
        return
    app.loader.import_default_modules()
    app.finalize(auto=True)
    _prepared.add(id(app))


def configure(app: Celery) -> None:
    """Points the app at the platform's queue; nothing happens off-platform.
    Call it after the application's own configuration::

        app.config_from_object("django.conf:settings", namespace="CELERY")
        ske_bridge.celery.configure(app)

    The application's own ``broker_transport_options`` are kept: the
    platform's region and queue are merged into them, and its queue joins
    any ``predefined_queues`` the application declares.
    """
    platform = settings()
    if not platform:
        return

    options = dict(app.conf.broker_transport_options or {})
    if platform.get("broker_transport"):
        app.conf.update({**platform, "broker_transport_options": {**options, **platform["broker_transport_options"]}})
        return
    queues = dict(options.get("predefined_queues") or {})
    queues.update(platform["broker_transport_options"]["predefined_queues"])
    options.update(platform["broker_transport_options"])
    options["predefined_queues"] = queues

    app.conf.update({**platform, "broker_transport_options": options})


def settings() -> dict[str, Any]:
    """The Celery settings that send tasks to the platform's queue, or an
    empty dict off-platform; configure() applies them.

    On AWS the queues are the environment's SQS queues: ``SQS_QUEUES`` names
    them, comma-separated, and each queue ``name`` is
    ``{SQS_PREFIX}/{name}{SQS_SUFFIX}``. ``SQS_QUEUE`` is the default queue,
    and the only one when ``SQS_QUEUES`` is unset or empty. All are declared
    as predefined queues, so Celery neither lists nor creates queues, and
    credentials come from the function's role through boto3's usual chain. A
    task routed to a queue the environment does not have fails to send,
    rather than going to a queue nothing reads.

    On GCP the queues are the environment's Cloud Tasks queues, which
    ``SKE_QUEUES`` names, each ``{SKE_QUEUE_PREFIX}{name}`` in
    ``SKE_GCP_PROJECT`` at ``SKE_REGION``. The bridge's own transport
    (``ske_bridge.celery.cloudtasks``) creates a task for each message, and
    a task routed to a queue not in ``SKE_QUEUES`` fails to send the same way.
    """
    if environment.get("SKE_PROVIDER") == "gcp":
        queues = [name.strip() for name in (environment.get("SKE_QUEUES") or "").split(",") if name.strip()]
        return {
            "broker_url": "cloudtasks://",
            "broker_transport": "ske_bridge.celery.cloudtasks:Transport",
            "broker_transport_options": {
                "project": environment.get("SKE_GCP_PROJECT"),
                "region": environment.get("SKE_REGION"),
                "prefix": environment.get("SKE_QUEUE_PREFIX"),
                "queues": queues,
            },
            "task_default_queue": "default",
        }
    if environment.get("SKE_PROVIDER") != "aws":
        return {}

    queue = environment.get("SQS_QUEUE") or "default"
    prefix = (environment.get("SQS_PREFIX") or "").rstrip("/")
    suffix = environment.get("SQS_SUFFIX") or ""
    region = environment.get("AWS_REGION") or environment.get("AWS_DEFAULT_REGION")

    names = [name.strip() for name in (environment.get("SQS_QUEUES") or "").split(",") if name.strip()]
    if queue not in names:
        names.insert(0, queue)

    return {
        "broker_url": "sqs://",
        "broker_transport_options": {
            "region": region,
            "predefined_queues": {name: {"url": f"{prefix}/{name}{suffix}"} for name in names},
        },
        "task_default_queue": queue,
    }
