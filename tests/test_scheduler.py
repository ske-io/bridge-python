import json
from datetime import UTC, datetime, timedelta

import pytest
from celery.schedules import BaseSchedule, crontab, schedule

from ske_bridge.celery.runner import QueueRunner
from ske_bridge.celery.scheduler import is_due, tick
from tests.conftest import envelope, record

# A Monday, 09:30 in Paris (UTC+2 in October).
MONDAY_0930_PARIS = datetime(2026, 10, 12, 7, 30, tzinfo=UTC)


def minute(*args):
    return datetime(*args, tzinfo=UTC)


@pytest.mark.parametrize(("sched", "at", "due"), [
    (crontab(), minute(2026, 10, 12, 7, 31), True),
    (crontab(minute=30, hour=9, day_of_week="mon-fri"), MONDAY_0930_PARIS, True),
    (crontab(minute=30, hour=9, day_of_week="mon-fri"), MONDAY_0930_PARIS + timedelta(minutes=1), False),
    (crontab(minute=30, hour=9, day_of_week="mon-fri"), MONDAY_0930_PARIS - timedelta(days=1), False),
    (crontab(minute=30, hour=7), MONDAY_0930_PARIS, False),
    (crontab(minute="*/15"), minute(2026, 10, 12, 7, 45), True),
    (crontab(minute=0, hour=0, day_of_month=1), minute(2026, 10, 31, 23, 0), True),
    (crontab(minute=0, hour=0, month_of_year=1), minute(2026, 10, 31, 23, 0), False),
])
def test_crontab_is_due_in_the_apps_timezone(celery_app, sched, at, due):
    assert is_due(sched, at, celery_app) is due


def test_seconds_within_the_minute_do_not_matter(celery_app):
    assert is_due(crontab(minute=30, hour=9), MONDAY_0930_PARIS + timedelta(seconds=59), celery_app)


@pytest.mark.parametrize(("every", "at", "due"), [
    (300, minute(2026, 10, 12, 7, 30), True),
    (300, minute(2026, 10, 12, 7, 31), False),
    (timedelta(hours=1), minute(2026, 10, 12, 8, 0), True),
    (timedelta(hours=1), minute(2026, 10, 12, 8, 1), False),
    (timedelta(days=1), minute(2026, 10, 12, 0, 0), True),
    (timedelta(days=1), minute(2026, 10, 12, 1, 0), False),
    (60, minute(2026, 10, 12, 7, 31), True),
    # Rounded down: 90s runs every minute, 150s every two, 179s every two.
    (90, minute(2026, 10, 12, 7, 31), True),
    (150, minute(2026, 10, 12, 7, 30), True),
    (150, minute(2026, 10, 12, 7, 31), False),
    (179, minute(2026, 10, 12, 7, 31), False),
    # Weekly intervals fall on Mondays at 00:00 UTC.
    (timedelta(weeks=1), minute(2026, 10, 12, 0, 0), True),
    (timedelta(weeks=1), minute(2026, 10, 15, 0, 0), False),
])
def test_an_interval_is_due_on_epoch_minutes_divisible_by_it(celery_app, every, at, due):
    sched = schedule(every if isinstance(every, timedelta) else timedelta(seconds=every))

    assert is_due(sched, at, celery_app) is due


def test_an_interval_under_a_minute_runs_every_tick_and_says_so(celery_app, caplog):
    assert is_due(schedule(timedelta(seconds=10)), minute(2026, 10, 12, 7, 31), celery_app, "fast")
    assert "'fast' runs every 10s" in caplog.text


class EveryOtherCall(BaseSchedule):
    """A custom schedule: is_due() is all the scheduler may ask of it."""

    def __init__(self, due):
        super().__init__()
        self.due = due
        self.asked = []

    def is_due(self, last_run_at):
        self.asked.append(last_run_at)
        return self.due, 60


@pytest.mark.parametrize("due", [True, False])
def test_other_schedules_are_asked_whether_they_came_due_in_the_last_minute(celery_app, due):
    sched = EveryOtherCall(due)

    assert is_due(sched, MONDAY_0930_PARIS, celery_app) is due
    assert sched.asked == [MONDAY_0930_PARIS - timedelta(minutes=1)]


class At(BaseSchedule):
    """A schedule with one event, at a given time."""

    def __init__(self, event):
        super().__init__(nowfun=lambda: datetime.now(UTC))
        self.event = event

    def remaining_estimate(self, last_run_at):
        return self.event - self.now() if self.event > last_run_at else timedelta(days=365)


@pytest.mark.parametrize(("offset", "due"), [
    (timedelta(0), True),
    (timedelta(seconds=59), True),
    (timedelta(minutes=1), False),
    (timedelta(seconds=-1), False),
])
def test_an_event_is_due_in_one_minute_only_start_included_end_excluded(celery_app, offset, due):
    assert is_due(At(MONDAY_0930_PARIS + offset), MONDAY_0930_PARIS, celery_app) is due


def test_tick_sends_the_due_entries(celery_app, sqs_sent):
    lines = []

    result = tick(celery_app, MONDAY_0930_PARIS, echo=lines.append)

    assert result.ok
    assert result.sent == ["every-minute", "every-five", "weekday-mornings"]
    sent = [envelope(m["MessageBody"]) for m in sqs_sent]
    assert [e["headers"]["task"] for e in sent] == ["tests.record"] * 3
    assert lines[0].startswith("sent every-minute (tests.record) as ")


def test_tick_off_the_minute_sends_less(celery_app, sqs_sent):
    result = tick(celery_app, MONDAY_0930_PARIS + timedelta(minutes=1), echo=lambda _: None)

    assert result.sent == ["every-minute"]


def test_tick_sends_an_unregistered_task_by_name(celery_app, sqs_sent, monkeypatch):
    monkeypatch.setitem(celery_app.conf.beat_schedule, "remote", {"task": "elsewhere.task", "schedule": 60})

    result = tick(celery_app, MONDAY_0930_PARIS + timedelta(minutes=1), echo=lambda _: None)

    assert result.sent == ["every-minute", "remote"]
    assert envelope(sqs_sent[-1]["MessageBody"])["headers"]["task"] == "elsewhere.task"


def test_a_failed_send_is_reported_and_does_not_stop_the_rest(celery_app, monkeypatch):
    monkeypatch.setitem(celery_app.conf.beat_schedule, "bad", {"task": "tests.record", "schedule": 60, "options": {"queue": "nowhere"}})
    lines = []

    result = tick(celery_app, MONDAY_0930_PARIS + timedelta(minutes=1), echo=lines.append)

    assert result.sent == ["every-minute"]
    assert [name for name, _ in result.failed] == ["bad"]
    assert not result.ok


def test_nothing_due_says_so(celery_app, monkeypatch):
    monkeypatch.setitem(celery_app.conf, "beat_schedule", {})
    lines = []

    tick(celery_app, MONDAY_0930_PARIS, echo=lines.append)

    assert lines == ["nothing due"]


def test_what_the_scheduler_sends_the_queue_runs(celery_app, sqs_sent):
    from tests.celery_app import calls

    tick(celery_app, MONDAY_0930_PARIS, echo=lambda _: None)
    runner = QueueRunner(celery_app)

    outcomes = runner.process([record(m["MessageBody"], message_id=str(i)) for i, m in enumerate(sqs_sent)])

    assert all(o.deleted for o in outcomes)
    assert calls == [("record", "minute"), ("record", "five"), ("record", "morning")]


@pytest.mark.parametrize(("value", "expected"), [
    ("2026-10-12T07:30:00Z", MONDAY_0930_PARIS),
    ("2026-10-12T07:30:42.5Z", MONDAY_0930_PARIS),
    ("2026-10-12T09:30:00+02:00", MONDAY_0930_PARIS),
])
def test_the_scheduled_minute_comes_from_the_platform(monkeypatch, value, expected):
    from ske_bridge.celery.scheduler import current_minute

    monkeypatch.setenv("SKE_SCHEDULED_AT", value)

    assert current_minute() == expected


@pytest.mark.parametrize("value", ["yesterday", "2026-10-12T07:30:00"])
def test_an_unusable_scheduled_time_falls_back_to_the_clock(monkeypatch, caplog, value):
    from ske_bridge.celery.scheduler import current_minute

    monkeypatch.setenv("SKE_SCHEDULED_AT", value)

    assert abs((current_minute() - datetime.now(UTC)).total_seconds()) < 61
    assert "SKE_SCHEDULED_AT" in caplog.text


def test_a_late_tick_sends_the_minute_it_was_scheduled_for(celery_app, sqs_sent, monkeypatch):
    monkeypatch.setenv("SKE_SCHEDULED_AT", "2026-10-12T07:30:00Z")

    result = tick(celery_app, echo=lambda _: None)

    assert result.sent == ["every-minute", "every-five", "weekday-mornings"]


def test_maintenance_sends_nothing_but_entries_marked_to_run_anyway(celery_app, sqs_sent, platform, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    monkeypatch.setitem(celery_app.conf.beat_schedule, "cleanup", {
        "task": "tests.record", "schedule": 60, "args": ("cleanup",), "options": {"ske_even_in_maintenance": True},
    })
    lines = []

    result = tick(celery_app, MONDAY_0930_PARIS, echo=lines.append)

    assert result.sent == ["cleanup"]
    assert result.skipped == ["every-minute", "every-five", "weekday-mornings"]
    assert "skipped every-minute (tests.record): maintenance mode is on" in lines
    sent = envelope(sqs_sent[-1]["MessageBody"])
    assert "ske_even_in_maintenance" not in sent["headers"] and "ske_even_in_maintenance" not in sent["properties"]


def test_the_marker_is_not_sent_outside_maintenance_either(celery_app, sqs_sent, monkeypatch):
    monkeypatch.setitem(celery_app.conf.beat_schedule, "cleanup", {
        "task": "tests.record", "schedule": 60, "options": {"ske_even_in_maintenance": True},
    })

    tick(celery_app, MONDAY_0930_PARIS + timedelta(minutes=1), echo=lambda _: None)

    assert all("ske_even_in_maintenance" not in json.dumps(envelope(m["MessageBody"])) for m in sqs_sent)
