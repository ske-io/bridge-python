"""The Celery beat schedule, without a beat process.

Once a minute the platform runs the scheduler command in the container (``python manage.py ske_schedule``,
``flask ske schedule``). Each run sends the ``beat_schedule`` entries that are
due in the current minute to the queue, as beat would, and exits. Nothing is
remembered between runs, so whether an entry is due is decided from the
minute alone:

- ``crontab``: due when the minute matches its fields, in the app's
  timezone. Day of month and day of week must both match, as in Celery.
- An interval (a number of seconds, a ``timedelta`` or ``schedule``): due
  when the minutes since ANCHOR, Monday 1970-01-05 00:00 UTC, are a multiple
  of the interval in whole minutes, rounded down. An hourly entry runs at the
  top of every hour, a daily one at 00:00 UTC and a weekly one on Mondays at
  00:00 UTC. A 90-second interval runs every minute. An interval under a
  minute cannot be kept and runs once a minute; each run says so in the log.
- Any other schedule (``solar``, a custom class): due when its next event
  after the start of the minute falls inside the minute, start included and
  end excluded, so an event on the boundary is sent by one tick only. A
  schedule that cannot estimate its next event is asked ``is_due()`` with
  the start of the previous minute as its last run.

The minute is the one the platform scheduled the run for, which it passes as
``SKE_SCHEDULED_AT``, so a run that starts late or is retried still sends that
minute's entries; without it the clock decides. A tick the platform delivers
twice sends its entries twice, and one it never delivers sends none: the
platform's scheduler delivers at least once, a minute at a time, and a task
that must not run twice guards itself as it would under beat with more than
one instance.

While the platform has the app in maintenance, no entry is sent unless its
options say ``"ske_even_in_maintenance": True`` (see EVEN_IN_MAINTENANCE).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ske_bridge import environment

logger = logging.getLogger("ske_bridge")

#: The beat entry option that sends an entry while the platform has the app in
#: maintenance: ``"options": {"ske_even_in_maintenance": True}``. It is taken
#: out before the task is sent.
EVEN_IN_MAINTENANCE = "ske_even_in_maintenance"

#: Where interval schedules are counted from: a Monday at midnight UTC, so a
#: weekly interval falls on Mondays and anything dividing a day on midnight.
ANCHOR = datetime(1970, 1, 5, tzinfo=UTC)


@dataclass
class TickResult:
    sent: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed


def current_minute() -> datetime:
    """The minute this run is for: the one the platform scheduled it for when
    it says (``SKE_SCHEDULED_AT``, an RFC 3339 time), so a run that starts
    late or is retried sends that minute's entries; else the clock's."""
    scheduled = scheduled_at()
    now = scheduled if scheduled is not None else datetime.now(UTC)
    return now.replace(second=0, microsecond=0)


def scheduled_at() -> datetime | None:
    value = environment.get("SKE_SCHEDULED_AT")
    if value is None:
        return None
    try:
        # From Python 3.11 fromisoformat() reads RFC 3339, a trailing "Z"
        # included.
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        logger.warning("ske: SKE_SCHEDULED_AT %r is not an RFC 3339 time; using the clock", value)
        return None
    if parsed.tzinfo is None:
        logger.warning("ske: SKE_SCHEDULED_AT %r has no UTC offset; using the clock", value)
        return None
    return parsed.astimezone(UTC)


def due_entries(app: Any, minute: datetime) -> list[tuple[str, dict[str, Any]]]:
    """The schedule's entries due in ``minute``, by name, in schedule order."""
    from celery.schedules import maybe_schedule

    due = []
    for name, entry in (app.conf.beat_schedule or {}).items():
        sched = maybe_schedule(entry["schedule"], entry.get("relative", False), app)
        if is_due(sched, minute, app, name):
            due.append((name, entry))
    return due


def is_due(sched: Any, minute: datetime, app: Any, name: str = "") -> bool:
    from celery.schedules import crontab, schedule

    minute = minute.astimezone(UTC).replace(second=0, microsecond=0)

    if isinstance(sched, crontab):
        local = minute.astimezone(app.timezone)
        return (
            local.minute in sched.minute
            and local.hour in sched.hour
            and local.isoweekday() % 7 in sched.day_of_week
            and local.day in sched.day_of_month
            and local.month in sched.month_of_year
        )

    if isinstance(sched, schedule):
        seconds = sched.run_every.total_seconds()
        if seconds < 60:
            logger.warning(
                "ske: beat entry %r runs every %gs, but the schedule runs once a minute; it runs once a minute",
                name, seconds,
            )
            return True
        every = max(int(seconds // 60), 1)
        return int((minute - ANCHOR).total_seconds() // 60) % every == 0

    # The next event after just before the minute starts, so one exactly on
    # the start counts. remaining_estimate() answers relative to the
    # schedule's clock, which is pinned for the call so the event's time
    # comes out exact.
    start = minute - timedelta(microseconds=1)
    pinned = datetime.now(UTC)
    nowfun = getattr(sched, "nowfun", None)
    sched.nowfun = lambda: pinned
    try:
        event = pinned + sched.remaining_estimate(start)
    except NotImplementedError:
        due, _ = sched.is_due(minute - timedelta(minutes=1))
        return bool(due)
    finally:
        sched.nowfun = nowfun
    event = event if event.tzinfo else event.replace(tzinfo=UTC)
    return minute <= event < minute + timedelta(minutes=1)


def tick(app: Any, minute: datetime | None = None, echo: Callable[[str], Any] = print) -> TickResult:
    """Sends the entries due in ``minute`` (this minute by default) and
    reports what was sent. An entry that fails to send does not stop the
    others."""
    from celery.beat import BeatLazyFunc

    from ske_bridge import maintenance

    paused = maintenance.active()
    result = TickResult()
    for name, entry in due_entries(app, minute or current_minute()):
        options = dict(entry.get("options") or {})
        even_in_maintenance = bool(options.pop(EVEN_IN_MAINTENANCE, False))
        if paused and not even_in_maintenance:
            result.skipped.append(name)
            echo(f"skipped {name} ({entry['task']}): maintenance mode is on")
            continue

        args = [a() if isinstance(a, BeatLazyFunc) else a for a in entry.get("args") or ()]
        kwargs = {k: v() if isinstance(v, BeatLazyFunc) else v for k, v in (entry.get("kwargs") or {}).items()}

        # As beat sends: through the task when this process has it, so its
        # routing and options apply, and by name when it does not.
        try:
            task = app.tasks.get(entry["task"])
            if task is not None:
                sent = task.apply_async(args, kwargs, **options)
            else:
                sent = app.send_task(entry["task"], args, kwargs, **options)
        except Exception as e:
            result.failed.append((name, f"{type(e).__name__}: {e}"))
            echo(f"failed {name} ({entry['task']}): {type(e).__name__}: {e}")
            continue

        result.sent.append(name)
        echo(f"sent {name} ({entry['task']}) as {sent.id}")

    if not result.sent and not result.failed and not result.skipped:
        echo("nothing due")
    return result
