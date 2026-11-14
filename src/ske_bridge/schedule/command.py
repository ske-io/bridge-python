"""What every scheduler command does once a minute: ``ske-bridge schedule``,
``python manage.py ske_schedule`` and ``flask ske schedule``.

For the minute the platform scheduled the run for (see
ske_bridge.celery.scheduler.current_minute), in this order:

1. The Celery beat entries due, sent to the queue (see
   ske_bridge.celery.scheduler), when the app has a Celery app. Sending is
   quick, so a slow task never holds them up.
2. The app's own scheduled tasks due (see ske_bridge.schedule), run here one
   after the other, each inside a new context of the framework
   integration's (see ske_bridge.schedule.tick).
3. Off the platform, the jobs those tasks dispatched (ske_bridge.jobs), which
   run in this process, and the jobs they dispatch in turn: waited for, at
   most LOCAL_JOBS_WAIT seconds, so the exit drops none. On the platform
   they went to the queue, and there is nothing to wait for.

The run fails when the beat schedule could not run (its Celery app is
broken) or an entry could not be sent, a task failed, or a job dispatched off
the platform failed or did not finish in the wait (one due to start after
the wait ends fails it at once). One failure does not stop the rest: the
tasks run even when the beat schedule could not.

An app with scheduled tasks needs no Celery app: when none can be found
(Celery is not installed, or no Celery app is configured), only its tasks
run. An app with neither fails, saying so.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ske_bridge import environment, schedule
from ske_bridge.jobs import local
from ske_bridge.schedule.tick import TickResult, tick

logger = logging.getLogger("ske_bridge")

#: How long, off the platform, the run waits for the jobs its tasks
#: dispatched, in seconds.
LOCAL_JOBS_WAIT = 60.0

NOTHING_SCHEDULED = (
    "no scheduled tasks are defined (is the module that defines them imported, e.g. from AppConfig.ready()?) "
    "and no Celery app is configured"
)


@dataclass(frozen=True)
class RunResult:
    #: The Celery beat tick, or None when it did not run.
    beat: Any
    tasks: TickResult
    #: Off the platform, what became of the jobs the run dispatched.
    jobs: local.Settled | None
    #: Why the beat schedule could not run, or why there was nothing to run.
    error: str | None = None

    @property
    def ok(self) -> bool:
        return (
            self.error is None and (self.beat is None or self.beat.ok) and self.tasks.ok
            and (self.jobs is None or self.jobs.ok)
        )

    @property
    def failure(self) -> str:
        """What failed, in one line; empty when nothing did."""
        parts = []
        if self.error is not None:
            parts.append(self.error)
        if self.beat is not None and self.beat.failed:
            parts.append(f"{len(self.beat.failed)} scheduled task(s) could not be sent")
        if self.tasks.failed:
            parts.append(f"{len(self.tasks.failed)} scheduled task(s) failed: {', '.join(self.tasks.failed)}")
        if self.jobs is not None and not self.jobs.ok:
            count = len(self.jobs.failed) + len(self.jobs.unfinished)
            parts.append(f"{count} job(s) dispatched by the schedule did not finish")
        return "; ".join(parts)


def open_local_jobs() -> None:
    """Off the platform, starts keeping track of the jobs dispatched from
    now on, so run() waits for them. A command that imports the app itself
    calls this before the import, so jobs dispatched as the app loads count
    too; run() calls it otherwise."""
    if not environment.running_on_platform():
        local.open_session()


def close_local_jobs() -> None:
    """Stops keeping track of jobs, for a run that ends before it waits for
    them."""
    if not environment.running_on_platform():
        local.settle(0)


def run(
    celery: Callable[[], Any] | None = None, *, echo: Callable[[str], Any] = print,
    minute: datetime | None = None, jobs_wait: float = LOCAL_JOBS_WAIT,
    around: Callable[[], AbstractContextManager[Any]] | None = None,
) -> RunResult:
    """One scheduler run; see the module. ``celery`` finds the app's Celery
    app, as ske_bridge.celery.find_app() does; None when the app has none
    to look for. ``around`` makes the context each task runs inside; see
    ske_bridge.schedule.tick."""
    from ske_bridge.celery.scheduler import current_minute

    open_local_jobs()
    try:
        minute = minute or current_minute()
        defined = schedule.tasks()
        beat, error = _beat(celery, bool(defined), minute, echo)
        tasks = tick(defined, minute, echo, report_idle=False, around=around)
    except BaseException:
        close_local_jobs()
        raise

    if error is None and tasks.idle and (beat is None or beat.idle):
        logger.debug("ske: nothing due")
    return RunResult(beat, tasks, _settle(jobs_wait, echo), error)


def _beat(celery: Callable[[], Any] | None, has_tasks: bool, minute: datetime, echo: Callable[[str], Any]) -> tuple[Any, str | None]:
    """Sends the beat entries due: the tick, or why it could not run."""
    if celery is None:
        return None, None
    try:
        app = celery()
    except Exception as e:
        if _no_celery(e):
            return None, None if has_tasks else NOTHING_SCHEDULED
        return None, _beat_failed(e, echo)

    from ske_bridge.celery.scheduler import tick as beat_tick

    try:
        return beat_tick(app, minute, echo, report_idle=False), None
    except Exception as e:
        return None, _beat_failed(e, echo)


def _beat_failed(error: Exception, echo: Callable[[str], Any]) -> str:
    logger.error("ske: the Celery beat schedule could not run", exc_info=True)
    echo(f"failed the beat schedule: {type(error).__name__}: {error}")
    return f"the Celery beat schedule could not run: {type(error).__name__}: {error}"


def _no_celery(error: Exception) -> bool:
    """Whether the error says the app has no Celery app, rather than that
    the one it names is broken."""
    from ske_bridge.celery import NoCeleryApp

    return isinstance(error, NoCeleryApp) or (isinstance(error, ModuleNotFoundError) and error.name == "celery")


def _settle(timeout: float, echo: Callable[[str], Any]) -> local.Settled | None:
    if environment.running_on_platform():
        return None
    settled = local.settle(timeout)
    for label in settled.unfinished:
        echo(f"job {label} did not finish: it was still pending or running {timeout:g}s after the schedule ran")
    for label in settled.failed:
        echo(f"job {label} failed")
    return settled
