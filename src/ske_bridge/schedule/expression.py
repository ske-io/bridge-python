"""Cron expressions, and whether one is due in a given minute.

Five fields (minute, hour, day of month, month, day of week) or six, with
seconds first. Each field takes ``*``, numbers, ranges (``1-5``), steps
(``*/15``, ``5/15``, ``1-30/5``) and lists of these; months and days take
names (``jan``, ``mon``), and Sunday is 0 or 7. The shorthands ``@yearly``,
``@annually``, ``@monthly``, ``@weekly``, ``@daily``, ``@midnight``,
``@hourly``, ``@minutely``, ``@secondly``, ``@weekdays`` and ``@weekends``
are read too.

When both day fields are restricted, a day matching either one is due, as in
classic cron. A day field is restricted when it leaves out some value: ``*``,
``*/1`` and ``1-31`` are all open.

Due-ness is decided from the minute alone, in the task's time zone: the
minute's local fields must match. The seconds field does not take part; an
expression matching more than one second of a minute is "second-level" and
can only run once a minute here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, tzinfo

SHORTHANDS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
    "@minutely": "* * * * *",
    "@secondly": "* * * * * *",
    "@weekdays": "0 0 * * 1-5",
    "@weekends": "0 0 * * 0,6",
}

MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
WEEKDAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]

# ASCII digits only: `\d` would take any Unicode digit.
_PART = re.compile(r"^(\*|\?|[^-/]+)(?:-([^/]+))?(?:/([0-9]+))?$")


class InvalidCron(ValueError):
    """The expression is not a cron expression this module reads."""


@dataclass(frozen=True)
class Cron:
    source: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    days_restricted: bool
    weekdays_restricted: bool
    #: Whether the expression matches more than one second of a minute.
    second_level: bool


def parse(expression: str) -> Cron:
    """Reads ``expression``; raises InvalidCron, naming what is wrong, when it
    cannot be read."""
    if not isinstance(expression, str):
        raise InvalidCron(f"{expression!r} is not a cron expression: it is not a string")
    source = expression.strip()
    fields = SHORTHANDS.get(source.lower(), source).split()
    if len(fields) not in (5, 6):
        raise InvalidCron(f"{expression!r} is not a cron expression: it needs 5 fields, or 6 with seconds first")
    second, minute, hour, day, month, weekday = fields if len(fields) == 6 else ["0", *fields]

    weekdays = _field(weekday, 0, 7, WEEKDAYS, expression)
    if 7 in weekdays:
        weekdays.discard(7)
        weekdays.add(0)
    days = _field(day, 1, 31, None, expression)

    return Cron(
        source=source,
        minutes=frozenset(_field(minute, 0, 59, None, expression)),
        hours=frozenset(_field(hour, 0, 23, None, expression)),
        days=frozenset(days),
        months=frozenset(_field(month, 1, 12, MONTHS, expression, name_base=1)),
        weekdays=frozenset(weekdays),
        # A day field is open when it covers every value, however it is
        # written (`*`, `*/1`, `1-31`, `0-6`).
        days_restricted=len(days) < 31,
        weekdays_restricted=len(weekdays) < 7,
        second_level=len(_field(second, 0, 59, None, expression)) > 1,
    )


def is_due(cron: Cron, at: datetime, zone: tzinfo | None = None) -> bool:
    """Whether ``cron`` is due in the minute ``at`` falls in, read in
    ``zone`` (the process's local time zone when None). ``at`` must be
    aware."""
    local = at.astimezone(zone)
    if local.minute not in cron.minutes or local.hour not in cron.hours or local.month not in cron.months:
        return False
    day = local.day in cron.days
    weekday = local.isoweekday() % 7 in cron.weekdays
    if cron.days_restricted and cron.weekdays_restricted:
        return day or weekday
    return day and weekday


def _field(field: str, low: int, high: int, names: list[str] | None, expression: str, name_base: int = 0) -> set[int]:
    def fail(why: str) -> InvalidCron:
        return InvalidCron(f"{expression!r} is not a cron expression: {why}")

    def value(text: str) -> int:
        lowered = text.lower()
        if names is not None and lowered in names:
            return names.index(lowered) + name_base
        if not text.isascii() or not text.isdigit():
            raise fail(f"{text!r} is not a number")
        return int(text)

    values: set[int] = set()
    for part in field.split(","):
        match = _PART.match(part)
        if match is None:
            raise fail(f"{part!r} cannot be read")
        start, end, step = match.groups()

        if start in ("*", "?"):
            if end is not None:
                raise fail(f"{part!r} cannot be read")
            first, last = low, high
        else:
            first = value(start)
            last = value(end) if end is not None else high if step is not None else first

        every = 1 if step is None else int(step)
        if every == 0:
            raise fail(f"{part!r} has a step of zero")
        if first < low or last > high or first > last:
            raise fail(f"{part!r} is outside {low}-{high}")
        values.update(range(first, last + 1, every))
    return values
