"""``ske-bridge schedule [--app module:attribute]``: the scheduler command
for an app that is neither Django nor Flask, which the platform runs once a
minute.

It imports the app named by ``--app``, else by ``SKE_APP``, as an import
string (``main:app``, ``service.api:app``), so the module defining it, and
the modules it imports, define their scheduled tasks (ske_bridge.schedule)
and jobs; the app itself is neither wrapped nor started. The current
directory is put first on ``sys.path``, as gunicorn does, so the app imports
as it does when served. Then it runs the schedule (see
ske_bridge.schedule.command): the tasks due this minute, and the Celery beat
entries due when ``SKE_CELERY_APP`` or ``CELERY_APP`` names a Celery app.

It refuses a Flask app, or an app whose import configures a Django
project: their schedules run with ``flask ske schedule`` and ``python
manage.py ske_schedule``. It exits 0 when everything due ran, 1 when
anything failed or the app could not be imported, and 2 on a usage error or
a Flask or Django app. With no app named and no Celery app
there is nothing to run, and it exits 0.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Sequence

from ske_bridge import environment

logger = logging.getLogger("ske_bridge")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ske-bridge", description="Commands the platform runs in the app's container.")
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")
    command = commands.add_parser(
        "schedule",
        help="Run the scheduled tasks due this minute, then exit.",
        description="Run the scheduled tasks due this minute, then exit.",
    )
    command.add_argument(
        "--app", metavar="MODULE:ATTRIBUTE",
        help="The app whose import defines the schedule, for example main:app. Defaults to SKE_APP.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return schedule(args.app or environment.get("SKE_APP"))


def schedule(target: str | None) -> int:
    from ske_bridge.logging import configure
    from ske_bridge.schedule import command

    configure()
    # Off the platform, from now on, so the jobs the app dispatches as it
    # is imported count too.
    command.open_local_jobs()

    celery = _celery_finder()
    if target is None:
        if celery is None:
            command.close_local_jobs()
            echo("no app named: set SKE_APP, or pass --app module:attribute, to run its scheduled tasks")
            return 0
    else:
        code = _import(target)
        if code:
            command.close_local_jobs()
            return code

    try:
        result = command.run(celery, echo=echo)
    except Exception:
        logger.error("ske: the schedule could not run", exc_info=True)
        return 1
    if not result.ok:
        echo(result.failure)
        return 1
    return 0


def _import(target: str) -> int:
    """Imports the app: 0 when it imported, 1 when it could not, 2 when it
    is a Flask app or a Django project, which have commands of their own."""
    from ske_bridge.generic import load

    cwd = os.getcwd()
    sys.path[:] = [cwd, *(entry for entry in sys.path if entry not in (cwd, ""))]
    try:
        app = load(target)
    except Exception:
        logger.error("ske: the app %s could not be imported", target, exc_info=True)
        return 1

    framework = _framework(app)
    if framework is not None:
        print(f"ske-bridge: {target} is {framework}", file=sys.stderr, flush=True)
        return 2
    return 0


def _framework(app: object) -> str | None:
    """Why the app's own framework command runs its schedule instead: for a
    Flask app, or once a Django project's settings are configured. Neither
    framework is imported here."""
    flask = sys.modules.get("flask")
    if flask is not None and isinstance(app, flask.Flask):
        return "a Flask app: run its schedule with `flask ske schedule`, with SkeBridge set up on the app"
    conf = sys.modules.get("django.conf")
    if conf is not None and conf.settings.configured:
        return "a Django project: run its schedule with `python manage.py ske_schedule`, with ske_bridge.django installed"
    return None


def _celery_finder():
    """Finds the Celery app ``SKE_CELERY_APP`` or ``CELERY_APP`` names, as the
    queue does for an app that is neither Django nor Flask; None without
    either."""
    if not (environment.get("SKE_CELERY_APP") or environment.get("CELERY_APP")):
        return None
    from ske_bridge.celery import find_app

    return find_app


def echo(line: str) -> None:
    print(line, flush=True)


if __name__ == "__main__":
    sys.exit(main())
