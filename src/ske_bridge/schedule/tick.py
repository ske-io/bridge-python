"""One run of the schedule: the tasks due in one minute, run one after the
other in this process, each to its end.

The minute is the one the platform scheduled the run for, which it passes as
``SKE_SCHEDULED_AT`` (an RFC 3339 time), so a run that starts late or is
retried runs that minute's tasks; without it the clock decides (see
ske_bridge.celery.scheduler.current_minute, which the Celery beat tick
shares). Nothing is remembered between runs, so due-ness is decided from the
minute alone (see ske_bridge.schedule.expression). A tick the platform
delivers twice runs its tasks twice: a task that must never run twice guards
itself.

While the app is in maintenance mode, a task runs only when it is marked
``even_in_maintenance=True``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ske_bridge.schedule import ScheduledTask
from ske_bridge.schedule.expression import is_due

logger = logging.getLogger("ske_bridge")


@dataclass
class TickResult:
    ran: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed

    @property
    def idle(self) -> bool:
        return not (self.ran or self.skipped or self.failed)


def tick(
    tasks: Iterable[ScheduledTask], minute: datetime, echo: Callable[[str], Any] = print, *,
    report_idle: bool = True, around: Callable[[], AbstractContextManager[Any]] | None = None,
) -> TickResult:
    """Runs the tasks due in ``minute``, in order, and reports what ran. A
    failing task does not stop the others, whatever it raises, except
    KeyboardInterrupt. Each runs inside a new context from ``around``, by
    default what the framework integration runs each job inside (a Flask app
    context, Django's connection upkeep; see ske_bridge.jobs). With
    ``report_idle``, a debug line says when nothing was due."""
    from ske_bridge import maintenance
    from ske_bridge.jobs import _around, _finish

    around = around or _around

    paused = maintenance.active()
    result = TickResult()
    second_level: list[str] = []

    for task in tasks:
        if not is_due(task.cron, minute, task.time_zone):
            continue
        if paused and not task.even_in_maintenance:
            result.skipped.append(task.name)
            echo(f"skipped {task.name}: maintenance mode is on")
            continue
        if task.cron.second_level:
            second_level.append(f"{task.name} ({task.expression})")

        started = time.monotonic()
        try:
            with around():
                _finish(task.run(), f"ske-task-{task.name}")
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            result.failed.append(task.name)
            echo(f"failed {task.name}: {type(e).__name__}: {e}")
            logger.error("ske: scheduled task %s failed", task.name, exc_info=True)
            continue
        result.ran.append(task.name)
        echo(f"ran {task.name} in {round((time.monotonic() - started) * 1000)}ms")

    # One line for the run, however many tasks: the same warning every
    # minute for each would drown the log.
    if second_level:
        logger.warning(
            "ske: %d scheduled task(s) run more than once a minute, but the schedule runs once a minute; "
            "each ran once this minute: %s",
            len(second_level), ", ".join(second_level),
        )
    if report_idle and result.idle:
        logger.debug("ske: nothing due")
    return result
