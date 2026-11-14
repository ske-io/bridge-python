"""Scheduled tasks, run by the platform's scheduler::

    from ske_bridge import schedule

    @schedule.cron("*/5 * * * *")
    def prune_sessions():
        ...

    @schedule.daily(at="02:00", time_zone="Europe/Paris", even_in_maintenance=True)
    async def send_report():
        ...

No timer runs in the application. Once a minute the platform runs the
scheduler command (``ske-bridge schedule``, ``python manage.py
ske_schedule``, ``flask ske schedule``), which imports the app, runs the
tasks due that minute one after the other, waits for each, and exits (see
ske_bridge.schedule.tick and ske_bridge.schedule.command).

Each decorator adds the function to the schedule and returns it unchanged;
it needs its parentheses (``@schedule.daily()``). A task is named
``name=``, else ``module.qualname``. Defining a taken name is an error, as
is an expression that cannot be read or an unknown time zone, so a mistake
shows when the app is imported rather than when the task should have run.
The one exception is the same module-level function defined again (the same
source file and ``__qualname__``), as a module imported a second time, or
under a second name, does: the newer definition replaces the older.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import tzinfo
from typing import Any, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ske_bridge.schedule.expression import Cron, InvalidCron, parse

__all__ = [
    "InvalidCron", "ScheduledTask", "cron", "daily", "every_minute", "hourly", "monthly",
    "tasks", "weekdays", "weekends", "weekly", "yearly",
]

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class ScheduledTask:
    name: str
    #: The expression as given.
    expression: str
    cron: Cron
    #: The zone the expression is read in; None for the process's own.
    time_zone: tzinfo | None
    even_in_maintenance: bool
    run: Callable[[], Any]


_tasks: dict[str, ScheduledTask] = {}
_lock = threading.Lock()


def tasks() -> list[ScheduledTask]:
    """The tasks defined in this process, in the order they were defined."""
    return list(_tasks.values())


def cron(
    expression: str, *, name: str | None = None, time_zone: str | tzinfo | None = None,
    even_in_maintenance: bool = False,
) -> Callable[[F], F]:
    """Runs the decorated function, sync or async and taking no arguments, in
    the minutes ``expression`` matches (see ske_bridge.schedule.expression).

    ``time_zone`` is the zone the expression is read in, an IANA name
    (``"Europe/Paris"``) or a ``tzinfo``; the process's own time zone by
    default, which on the platform is UTC unless ``TZ`` says otherwise.
    ``even_in_maintenance`` runs the task while the app is in maintenance
    mode too."""
    _refuse_bare("cron", expression)
    parsed = parse(expression)
    zone = _zone(time_zone)

    def define(handler: F) -> F:
        if not callable(handler):
            raise TypeError(f"scheduled task {name or expression!r} needs a function")
        task_name = name if name is not None else _default_name(handler)
        if not isinstance(task_name, str) or task_name == "":
            raise TypeError("a scheduled task needs a name: pass name=")
        origin = _origin(handler)
        with _lock:
            if name is None and origin is not None:
                # The same file under another module name has another
                # default name: it is still the task defined before.
                for defined in _tasks.values():
                    if _origin(defined.run) == origin and defined.name == _default_name(defined.run):
                        task_name = defined.name
                        break
            task = ScheduledTask(task_name, expression, parsed, zone, bool(even_in_maintenance), handler)
            existing = _tasks.get(task_name)
            if existing is not None and (origin is None or _origin(existing.run) != origin):
                raise ValueError(f"a scheduled task named {task_name!r} is already defined")
            _tasks[task_name] = task
        return handler

    return define


def every_minute(*args: Any, **options: Any) -> Callable[[F], F]:
    """Every minute (``* * * * *``)."""
    if args:
        _refuse_bare("every_minute", args[0])
        raise TypeError("schedule.every_minute() takes no positional arguments")
    return cron("* * * * *", **options)


def hourly(minute: int = 0, **options: Any) -> Callable[[F], F]:
    """Every hour, at ``minute`` past (``0 * * * *`` by default)."""
    _refuse_bare("hourly", minute)
    return cron(f"{_number(minute, 0, 59, 'minute')} * * * *", **options)


def daily(at: str = "00:00", **options: Any) -> Callable[[F], F]:
    """Every day at ``at``, ``"HH:MM"`` (``0 0 * * *`` by default)."""
    _refuse_bare("daily", at)
    hour, minute = _time(at)
    return cron(f"{minute} {hour} * * *", **options)


def weekdays(at: str = "00:00", **options: Any) -> Callable[[F], F]:
    """Monday to Friday at ``at`` (``0 0 * * 1-5`` by default)."""
    _refuse_bare("weekdays", at)
    hour, minute = _time(at)
    return cron(f"{minute} {hour} * * 1-5", **options)


def weekends(at: str = "00:00", **options: Any) -> Callable[[F], F]:
    """Saturday and Sunday at ``at`` (``0 0 * * 0,6`` by default)."""
    _refuse_bare("weekends", at)
    hour, minute = _time(at)
    return cron(f"{minute} {hour} * * 0,6", **options)


def weekly(day: int | str = "sun", at: str = "00:00", **options: Any) -> Callable[[F], F]:
    """Once a week, on ``day`` (0 or 7 for Sunday to 6, or a name: ``"mon"``)
    at ``at`` (``0 0 * * 0`` by default)."""
    _refuse_bare("weekly", day)
    hour, minute = _time(at)
    if isinstance(day, bool) or not isinstance(day, int | str):
        raise TypeError(f"a weekly task's day is a number or a name, not {day!r}")
    return cron(f"{minute} {hour} * * {day}", **options)


def monthly(day: int = 1, at: str = "00:00", **options: Any) -> Callable[[F], F]:
    """Once a month, on ``day`` (1 to 31; a month without that day is
    skipped) at ``at`` (``0 0 1 * *`` by default)."""
    _refuse_bare("monthly", day)
    hour, minute = _time(at)
    return cron(f"{minute} {hour} {_number(day, 1, 31, 'day')} * *", **options)


def yearly(month: int = 1, day: int = 1, at: str = "00:00", **options: Any) -> Callable[[F], F]:
    """Once a year, on ``day`` of ``month`` (1 to 12) at ``at`` (``0 0 1 1 *``
    by default)."""
    _refuse_bare("yearly", month)
    hour, minute = _time(at)
    return cron(f"{minute} {hour} {_number(day, 1, 31, 'day')} {_number(month, 1, 12, 'month')} *", **options)


def _refuse_bare(helper: str, value: Any) -> None:
    """A decorator used without its parentheses gets the function as its
    first argument."""
    if callable(value) and not isinstance(value, str):
        raise TypeError(f"schedule.{helper} needs its parentheses: write @schedule.{helper}(...), not @schedule.{helper}")


def _default_name(handler: Any) -> str | None:
    """``module.qualname``, so functions of the same name in two modules are
    two tasks."""
    module, qualname = getattr(handler, "__module__", None), getattr(handler, "__qualname__", None)
    if not qualname:
        return None
    return f"{module}.{qualname}" if module else qualname


def _zone(time_zone: str | tzinfo | None) -> tzinfo | None:
    if time_zone is None or isinstance(time_zone, tzinfo):
        return time_zone
    if not isinstance(time_zone, str):
        raise TypeError(f"a time zone is an IANA name or a tzinfo, not {time_zone!r}")
    try:
        return ZoneInfo(time_zone)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(
            f"unknown time zone {time_zone!r}: name an IANA time zone, such as Europe/Paris "
            "(where the system has no time zone database, install tzdata)"
        ) from None


def _time(at: str) -> tuple[int, int]:
    """``"HH:MM"`` as (hour, minute)."""
    hour, sep, minute = at.partition(":") if isinstance(at, str) else ("", "", "")
    if sep and hour.isascii() and hour.isdigit() and minute.isascii() and minute.isdigit() and len(minute) == 2 and len(hour) <= 2:
        if int(hour) <= 23 and int(minute) <= 59:
            return int(hour), int(minute)
    raise ValueError(f"a time of day is HH:MM, from 00:00 to 23:59, not {at!r}")


def _number(value: int, low: int, high: int, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"a {what} is a whole number from {low} to {high}, not {value!r}")
    return value


def _origin(handler: Any) -> tuple[str, str] | None:
    """Where a module-level function or method is defined: its source file
    and qualified name, so the same file imported under two module names
    (``main`` and ``__main__``, ``app.tasks`` and ``tasks``) is one
    definition. None for a lambda, a function defined inside another, or a
    callable that is not a function, which no two definitions share."""
    code = getattr(handler, "__code__", None)
    filename, qualname = getattr(code, "co_filename", None), getattr(handler, "__qualname__", None)
    return (filename, qualname) if filename and qualname and "<" not in qualname else None
