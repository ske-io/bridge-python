import os
import time
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from ske_bridge.schedule.expression import InvalidCron, is_due, parse


def at(*args):
    return datetime(*args, tzinfo=UTC)


def due_days(expression, year, month):
    """The days of a month, at midnight UTC, the expression is due on."""
    cron = parse(expression)
    days = []
    day = at(year, month, 1)
    while day.month == month:
        if is_due(cron, day, UTC):
            days.append(day.day)
        day += timedelta(days=1)
    return days


# January 2028 starts on a Saturday: Mondays are the 3rd, 10th, 17th, 24th
# and 31st; Fridays the 7th, 14th, 21st and 28th.
MONDAYS = [3, 10, 17, 24, 31]


@pytest.mark.parametrize(("expression", "days"), [
    ("0 0 * * *", list(range(1, 32))),
    # Every day of the month written as a step or a range is open, so the
    # day of the week alone decides.
    ("0 0 */1 * 1", MONDAYS),
    ("0 0 1-31 * 1", MONDAYS),
    # Every day of the week written as a range or a step is open, so the day
    # of the month alone decides.
    ("0 0 1 * 0-6", [1]),
    ("0 0 1 * 1-7", [1]),
    ("0 0 1 * 0-7", [1]),
    ("0 0 1 * */1", [1]),
    ("0 0 1 * *", [1]),
    # Sunday is 0 or 7.
    ("0 0 * * 0", [2, 9, 16, 23, 30]),
    ("0 0 * * 7", [2, 9, 16, 23, 30]),
    # Both restricted: either one.
    ("0 0 */2 * 1", sorted(set(range(1, 32, 2)) | set(MONDAYS))),
    ("0 0 1,15 * mon", sorted({1, 15, *MONDAYS})),
    ("0 0 13 * fri", [7, 13, 14, 21, 28]),
    ("0 0 1-7 * 5", [1, 2, 3, 4, 5, 6, 7, 14, 21, 28]),
    ("0 0 * * mon-fri", [d for d in range(1, 32) if d not in (1, 2, 8, 9, 15, 16, 22, 23, 29, 30)]),
    ("0 0 * * MON,Wed", sorted(MONDAYS + [5, 12, 19, 26])),
    ("0 0 5/7 * *", [5, 12, 19, 26]),
    ("0 0 31 * *", [31]),
    ("@weekly", [2, 9, 16, 23, 30]),
    ("@weekdays", [d for d in range(1, 32) if d not in (1, 2, 8, 9, 15, 16, 22, 23, 29, 30)]),
    ("@weekends", [1, 2, 8, 9, 15, 16, 22, 23, 29, 30]),
])
def test_the_day_fields_in_january_2028(expression, days):
    assert due_days(expression, 2028, 1) == days


def test_a_day_a_month_does_not_have_is_never_due():
    assert due_days("0 0 31 * *", 2028, 4) == []
    assert due_days("0 0 29 feb *", 2028, 2) == [29]
    assert due_days("0 0 29 2 *", 2027, 2) == []


def test_months_by_number_step_and_name():
    cron = parse("0 0 1 */3 *")
    assert sorted(cron.months) == [1, 4, 7, 10]
    assert sorted(parse("0 0 1 jan-mar,DEC *").months) == [1, 2, 3, 12]
    assert sorted(parse("0 0 1 feb/4 *").months) == [2, 6, 10]


@pytest.mark.parametrize(("expression", "minutes", "hours"), [
    ("*/15 * * * *", {0, 15, 30, 45}, set(range(24))),
    ("5/20 * * * *", {5, 25, 45}, set(range(24))),
    ("1-30/10 * * * *", {1, 11, 21}, set(range(24))),
    ("0 */3 * * *", {0}, {0, 3, 6, 9, 12, 15, 18, 21}),
    ("30 9-17 * * *", {30}, set(range(9, 18))),
    ("0,30 8,20 * * *", {0, 30}, {8, 20}),
    ("59 23 * * *", {59}, {23}),
    ("? * * * *", set(range(60)), set(range(24))),
])
def test_minutes_and_hours(expression, minutes, hours):
    cron = parse(expression)
    assert (set(cron.minutes), set(cron.hours)) == (minutes, hours)


def test_due_is_decided_from_the_minute():
    cron = parse("30 9 * * *")
    assert is_due(cron, at(2026, 3, 4, 9, 30), UTC)
    assert is_due(cron, at(2026, 3, 4, 9, 30, 59, 999999), UTC)
    assert not is_due(cron, at(2026, 3, 4, 9, 31), UTC)
    assert not is_due(cron, at(2026, 3, 4, 10, 30), UTC)


@pytest.mark.parametrize(("shorthand", "expression"), [
    ("@yearly", "0 0 1 1 *"),
    ("@annually", "0 0 1 1 *"),
    ("@monthly", "0 0 1 * *"),
    ("@weekly", "0 0 * * 0"),
    ("@daily", "0 0 * * *"),
    ("@midnight", "0 0 * * *"),
    ("@hourly", "0 * * * *"),
    ("@minutely", "* * * * *"),
    ("@DAILY", "0 0 * * *"),
    (" @hourly ", "0 * * * *"),
])
def test_shorthands(shorthand, expression):
    a, b = parse(shorthand), parse(expression)
    assert (a.minutes, a.hours, a.days, a.months, a.weekdays) == (b.minutes, b.hours, b.days, b.months, b.weekdays)


def test_six_fields_take_seconds_first_and_say_when_they_match_more_than_one_second():
    assert not parse("* * * * *").second_level
    assert not parse("10 * * * * *").second_level
    assert parse("*/30 * * * * *").second_level
    assert parse("@secondly").second_level
    # The seconds field does not take part in due-ness.
    assert is_due(parse("10 30 9 * * *"), at(2026, 3, 4, 9, 30), UTC)


@pytest.mark.parametrize(("expression", "why"), [
    ("61 * * * *", "outside 0-59"),
    ("* 24 * * *", "outside 0-23"),
    ("* * 0 * *", "outside 1-31"),
    ("* * * 13 *", "outside 1-12"),
    ("* * * * 8", "outside 0-7"),
    ("5-1 * * * *", "outside"),
    ("*/0 * * * *", "step of zero"),
    ("* * *", "5 fields"),
    ("* * * * * * *", "5 fields"),
    ("", "5 fields"),
    ("@often", "5 fields"),
    ("x * * * *", "'x' is not a number"),
    ("mon * * * *", "'mon' is not a number"),
    ("1.5 * * * *", "is not a number"),
    ("*-5 * * * *", "cannot be read"),
    ("1,,2 * * * *", "cannot be read"),
    ("1/x * * * *", "cannot be read"),
    ("٣ * * * *", "is not a number"),
    ("*/٣ * * * *", "cannot be read"),
])
def test_what_cannot_be_read_is_refused(expression, why):
    with pytest.raises(InvalidCron, match=why):
        parse(expression)


def test_an_invalid_expression_is_a_value_error():
    with pytest.raises(ValueError, match="not a cron expression"):
        parse("61 * * * *")


def test_a_time_zone_is_read_at_its_local_time_across_daylight_saving():
    paris = ZoneInfo("Europe/Paris")
    nine = parse("0 9 * * *")
    # 09:00 in Paris is 08:00 UTC in winter and 07:00 UTC in summer.
    assert is_due(nine, at(2026, 1, 15, 8, 0), paris)
    assert not is_due(nine, at(2026, 1, 15, 9, 0), paris)
    assert is_due(nine, at(2026, 7, 15, 7, 0), paris)
    assert not is_due(nine, at(2026, 7, 15, 8, 0), paris)


def test_when_clocks_go_back_the_repeated_minute_is_due_twice_and_when_they_go_forward_the_skipped_one_never_is():
    new_york = ZoneInfo("America/New_York")
    # 2026-11-01: 01:30 happens at 05:30 UTC (EDT) and 06:30 UTC (EST).
    assert is_due(parse("30 1 * * *"), at(2026, 11, 1, 5, 30), new_york)
    assert is_due(parse("30 1 * * *"), at(2026, 11, 1, 6, 30), new_york)
    # 2026-03-08: 02:00 jumps to 03:00, so 02:30 never happens.
    skipped = parse("30 2 * * *")
    minute = at(2026, 3, 8, 0, 0)
    while minute < at(2026, 3, 9):
        assert not is_due(skipped, minute, new_york), minute
        minute += timedelta(minutes=30)
    assert is_due(skipped, at(2026, 3, 9, 6, 30), new_york)


def test_a_fixed_offset_and_the_day_it_falls_on():
    kolkata = timezone(timedelta(hours=5, minutes=30))
    # 20:00 UTC on Sunday the 1st is 01:30 on Monday the 2nd in UTC+05:30.
    assert is_due(parse("30 1 2 * mon"), at(2026, 3, 1, 20, 0), kolkata)
    assert not is_due(parse("30 1 * * sun"), at(2026, 3, 1, 20, 0), kolkata)


def test_without_a_zone_the_process_time_zone_decides():
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Tokyo"
    time.tzset()
    try:
        # 09:00 in Tokyo is 00:00 UTC.
        assert is_due(parse("0 9 * * *"), at(2026, 3, 4, 0, 0))
        assert not is_due(parse("0 9 * * *"), at(2026, 3, 4, 9, 0))
    finally:
        if previous is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = previous
        time.tzset()


def test_every_vector_from_the_nodejs_bridge():
    """Due-ness in thousands of minutes, four time zones and the days their
    clocks change, as the Node.js bridge decides it; see
    fixtures/cron_vectors.mjs, which wrote them."""
    import json

    from tests.conftest import FIXTURES

    vectors = json.loads((FIXTURES / "cron_vectors.json").read_text())
    minutes = [datetime.fromtimestamp(minute * 60, UTC) for minute in vectors["minutes"]]
    assert len(vectors["cases"]) >= 100

    for case in vectors["cases"]:
        cron, zone = parse(case["expression"]), ZoneInfo(case["zone"])
        expected = bin(int(case["due"], 16))[2:].zfill(len(case["due"]) * 4)[: len(minutes)]
        for minute, due in zip(minutes, expected, strict=True):
            assert is_due(cron, minute, zone) is (due == "1"), f"{case['expression']} in {case['zone']} at {minute.isoformat()}"
