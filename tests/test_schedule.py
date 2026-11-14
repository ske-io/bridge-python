import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from ske_bridge import schedule
from ske_bridge.celery import NoCeleryApp
from ske_bridge.jobs import job
from ske_bridge.schedule.command import run
from ske_bridge.schedule.tick import tick
from tests.conftest import define_task as define

# A Wednesday.
WEDNESDAY_1001 = datetime(2026, 3, 4, 10, 1, tzinfo=UTC)


def noop():
    pass


# Defining tasks


def test_a_decorator_adds_the_task_and_returns_the_function(scheduled_tasks):
    def prune():
        return "pruned"

    assert schedule.cron("*/5 * * * *")(prune) is prune
    [task] = schedule.tasks()
    assert (task.name, task.expression, task.time_zone, task.even_in_maintenance, task.run) == ("tests.test_schedule.test_a_decorator_adds_the_task_and_returns_the_function.<locals>.prune", "*/5 * * * *", None, False, prune)
    assert prune() == "pruned"


def test_tasks_keep_the_order_they_were_defined_in():
    for name in ("b", "a", "c"):
        define("* * * * *", name)
    assert [task.name for task in schedule.tasks()] == ["b", "a", "c"]


@pytest.mark.parametrize(("decorator", "expression"), [
    (schedule.every_minute(), "* * * * *"),
    (schedule.hourly(), "0 * * * *"),
    (schedule.hourly(minute=45), "45 * * * *"),
    (schedule.daily(), "0 0 * * *"),
    (schedule.daily(at="02:30"), "30 2 * * *"),
    (schedule.daily(at="9:05"), "5 9 * * *"),
    (schedule.weekdays(at="08:00"), "0 8 * * 1-5"),
    (schedule.weekends(), "0 0 * * 0,6"),
    (schedule.weekly(), "0 0 * * sun"),
    (schedule.weekly(day="mon", at="09:00"), "0 9 * * mon"),
    (schedule.weekly(day=5), "0 0 * * 5"),
    (schedule.monthly(), "0 0 1 * *"),
    (schedule.monthly(day=15, at="12:00"), "0 12 15 * *"),
    (schedule.yearly(), "0 0 1 1 *"),
    (schedule.yearly(month=7, day=14, at="06:00"), "0 6 14 7 *"),
])
def test_each_helper_is_a_cron_expression(decorator, expression):
    decorator(noop)
    [task] = schedule.tasks()
    assert task.expression == expression


def test_the_helpers_take_the_options():
    schedule.daily(at="02:00", name="report", time_zone="Europe/Paris", even_in_maintenance=True)(noop)
    [task] = schedule.tasks()
    assert (task.name, task.time_zone, task.even_in_maintenance) == ("report", ZoneInfo("Europe/Paris"), True)


@pytest.mark.parametrize("make", [
    lambda: schedule.daily(at="24:00"),
    lambda: schedule.daily(at="12:60"),
    lambda: schedule.daily(at="noon"),
    lambda: schedule.daily(at="12:5"),
    lambda: schedule.hourly(minute=60),
    lambda: schedule.monthly(day=0),
    lambda: schedule.monthly(day=32),
    lambda: schedule.yearly(month=13),
    lambda: schedule.weekly(day=8),
    lambda: schedule.weekly(day="someday"),
])
def test_a_helper_refuses_what_is_out_of_range(make):
    with pytest.raises(ValueError):
        make()


def test_a_bad_expression_or_time_zone_is_refused_when_the_app_loads():
    with pytest.raises(ValueError, match="not a cron expression"):
        schedule.cron("61 * * * *")
    with pytest.raises(ValueError, match="5 fields"):
        schedule.cron("* * *")
    with pytest.raises(ValueError, match="unknown time zone 'Mars/Olympus'"):
        schedule.cron("* * * * *", time_zone="Mars/Olympus")
    with pytest.raises(TypeError, match="needs a function"):
        schedule.cron("* * * * *")("not a function")
    assert schedule.tasks() == []


def test_a_taken_name_is_an_error():
    define("* * * * *", "taken")

    with pytest.raises(ValueError, match="'taken' is already defined"):
        define("0 * * * *", "taken")
    assert [task.expression for task in schedule.tasks()] == ["* * * * *"]


def load_as(module, path, expression="* * * * *", name=None):
    """Runs a module's source as ``module`` from the file ``path``, as an
    import does, and returns the task function it defines."""
    source = f"@schedule.cron({expression!r}, name={name!r})\ndef cleanup():\n    pass\n"
    namespace = {"__name__": module, "schedule": schedule}
    exec(compile(source, path, "exec"), namespace)
    return namespace["cleanup"]


def test_functions_of_the_same_name_in_two_modules_are_two_tasks():
    load_as("shop.tasks", "/app/shop/tasks.py")
    load_as("blog.tasks", "/app/blog/tasks.py")

    assert [task.name for task in schedule.tasks()] == ["shop.tasks.cleanup", "blog.tasks.cleanup"]


def test_the_same_file_defined_again_replaces_the_first_whatever_its_module_name():
    # A module imported a second time, or under a second name (``main`` and
    # ``__main__``, ``shop.tasks`` and ``tasks``), defines its functions again.
    load_as("shop.tasks", "/app/shop/tasks.py")
    again = load_as("tasks", "/app/shop/tasks.py", expression="0 * * * *")

    [task] = schedule.tasks()
    assert (task.name, task.run, task.expression) == ("shop.tasks.cleanup", again, "0 * * * *")

    again = load_as("tasks", "/app/shop/tasks.py", name="named")
    load_as("shop.tasks", "/app/shop/tasks.py", name="named")
    assert [task.name for task in schedule.tasks()] == ["shop.tasks.cleanup", "named"]


def test_another_file_under_a_taken_name_is_an_error():
    load_as("shop.tasks", "/app/shop/tasks.py", name="cleanup")

    with pytest.raises(ValueError, match="'cleanup' is already defined"):
        load_as("blog.tasks", "/app/blog/tasks.py", name="cleanup")


@pytest.mark.parametrize("helper", ["cron", "every_minute", "hourly", "daily", "weekdays", "weekends", "weekly", "monthly", "yearly"])
def test_a_decorator_without_its_parentheses_says_so(helper):
    with pytest.raises(TypeError, match=f"schedule.{helper} needs its parentheses: write @schedule.{helper}\\(...\\)"):
        getattr(schedule, helper)(noop)
    assert schedule.tasks() == []


def test_a_tzinfo_is_taken_as_it_is():
    zone = timezone(timedelta(hours=2))
    schedule.cron("0 12 * * *", name="noon", time_zone=zone)(noop)

    [task] = schedule.tasks()
    assert task.time_zone is zone
    assert tick(schedule.tasks(), datetime(2026, 3, 4, 10, 0, tzinfo=UTC), echo=lambda _: None).ran == ["noon"]


# A tick


def test_a_tick_runs_the_due_tasks_in_order_and_waits_for_each():
    order = []
    define("* * * * *", "every-minute", lambda: order.append("every-minute"))
    define("0 * * * *", "hourly", lambda: order.append("hourly"))
    define("1 10 * * wed", "wednesday", lambda: order.append("wednesday"), time_zone="UTC")
    lines = []

    result = tick(schedule.tasks(), WEDNESDAY_1001, echo=lines.append)

    assert order == result.ran == ["every-minute", "wednesday"]
    assert [line.split(" in ")[0] for line in lines] == ["ran every-minute", "ran wednesday"]
    assert result.ok


def test_an_async_task_is_run_to_its_end():
    seen = []

    @schedule.every_minute(name="async")
    async def handler():
        await asyncio.sleep(0.01)
        seen.append("done")

    assert tick(schedule.tasks(), WEDNESDAY_1001, echo=lambda _: None).ran == ["async"]
    assert seen == ["done"]

    # From code that already runs an event loop too.
    async def from_a_loop():
        return tick(schedule.tasks(), WEDNESDAY_1001, echo=lambda _: None)

    assert asyncio.run(from_a_loop()).ran == ["async"]
    assert seen == ["done", "done"]


def test_a_failing_task_does_not_stop_the_others(caplog):
    def broken():
        raise RuntimeError("the task broke")

    define("* * * * *", "broken", broken)
    define("* * * * *", "after")
    lines = []

    with caplog.at_level("ERROR", logger="ske_bridge"):
        result = tick(schedule.tasks(), WEDNESDAY_1001, echo=lines.append)

    assert (result.failed, result.ran, result.ok) == (["broken"], ["after"], False)
    assert lines[0] == "failed broken: RuntimeError: the task broke"
    [logged] = caplog.records
    assert logged.getMessage() == "ske: scheduled task broken failed"
    assert str(logged.exc_info[1]) == "the task broke"


def test_in_maintenance_only_tasks_marked_to_run_anyway_run(platform, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    define("* * * * *", "paused")
    define("* * * * *", "anyway", even_in_maintenance=True)
    lines = []

    result = tick(schedule.tasks(), WEDNESDAY_1001, echo=lines.append)

    assert (result.ran, result.skipped, result.ok) == (["anyway"], ["paused"], True)
    assert lines[0] == "skipped paused: maintenance mode is on"


def test_maintenance_mode_is_the_platforms_only(monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    define("* * * * *", "local")

    assert tick(schedule.tasks(), WEDNESDAY_1001, echo=lambda _: None).ran == ["local"]


def test_a_second_level_task_runs_once_with_one_warning_naming_it(caplog):
    calls = []
    define("*/30 * * * * *", "every-thirty-seconds", lambda: calls.append(1))
    define("10 * * * * *", "at-second-ten")

    with caplog.at_level("WARNING", logger="ske_bridge"):
        tick(schedule.tasks(), WEDNESDAY_1001, echo=lambda _: None)

    assert calls == [1]
    [warning] = caplog.records
    assert "more than once a minute" in warning.getMessage()
    assert "every-thirty-seconds (*/30 * * * * *)" in warning.getMessage()
    assert "at-second-ten" not in warning.getMessage()


def test_a_task_that_raises_system_exit_or_is_cancelled_fails_and_the_rest_run():
    def exits():
        raise SystemExit(3)

    async def cancelled():
        raise asyncio.CancelledError()

    def interrupted():
        raise KeyboardInterrupt

    define("* * * * *", "exits", exits)
    define("* * * * *", "cancelled", cancelled)
    define("* * * * *", "after")
    lines = []

    result = tick(schedule.tasks(), WEDNESDAY_1001, echo=lines.append)

    assert (result.failed, result.ran) == (["exits", "cancelled"], ["after"])
    assert lines[:2] == ["failed exits: SystemExit: 3", "failed cancelled: CancelledError: "]

    define("* * * * *", "interrupted", interrupted)
    with pytest.raises(KeyboardInterrupt):
        tick(schedule.tasks(), WEDNESDAY_1001, echo=lambda _: None)


def test_each_task_runs_inside_a_new_context_of_the_integrations():
    from contextlib import contextmanager

    from ske_bridge import jobs

    events = []

    @contextmanager
    def around():
        events.append("enter")
        yield
        events.append("exit")

    jobs.set_around_job(around)
    define("* * * * *", "one", lambda: events.append("one"))
    define("* * * * *", "two", lambda: events.append("two"))

    tick(schedule.tasks(), WEDNESDAY_1001, echo=lambda _: None)

    assert events == ["enter", "one", "exit", "enter", "two", "exit"]


def test_nothing_due_is_a_debug_line(caplog):
    define("0 * * * *", "hourly")
    lines = []

    with caplog.at_level("DEBUG", logger="ske_bridge"):
        assert tick(schedule.tasks(), WEDNESDAY_1001, echo=lines.append).idle

    assert lines == []
    assert [record.getMessage() for record in caplog.records] == ["ske: nothing due"]


# A run: the beat schedule and the tasks


def test_a_run_sends_the_beat_entries_first_then_runs_the_tasks(celery_app, sqs_sent):
    from tests.test_scheduler import MONDAY_0930_PARIS

    lines = []
    define("* * * * *", "native", lambda: lines.append(f"native saw {len(sqs_sent)} sent"))

    result = run(lambda: celery_app, echo=lines.append, minute=MONDAY_0930_PARIS)

    assert result.ok
    assert result.beat.sent == ["every-minute", "every-five", "weekday-mornings"]
    assert result.tasks.ran == ["native"]
    assert [line.split(" ")[0] for line in lines] == ["sent", "sent", "sent", "native", "ran"]
    assert lines[3] == "native saw 3 sent"


def test_a_run_fails_when_either_fails_and_one_does_not_stop_the_other(celery_app, monkeypatch):
    monkeypatch.setitem(celery_app.conf, "beat_schedule", {"bad": {"task": "tests.record", "schedule": 60, "options": {"queue": "nowhere"}}})
    define("* * * * *", "native")

    result = run(lambda: celery_app, echo=lambda _: None, minute=WEDNESDAY_1001)

    assert not result.ok
    assert result.tasks.ran == ["native"]
    assert result.failure == "1 scheduled task(s) could not be sent"

    define("* * * * *", "broken", lambda: 1 / 0)
    monkeypatch.setitem(celery_app.conf, "beat_schedule", {})
    result = run(lambda: celery_app, echo=lambda _: None, minute=WEDNESDAY_1001)
    assert result.failure == "1 scheduled task(s) failed: broken"


def test_a_run_says_nothing_due_once_at_debug(celery_app, monkeypatch, caplog):
    monkeypatch.setitem(celery_app.conf, "beat_schedule", {})
    define("0 * * * *", "hourly")
    lines = []

    with caplog.at_level("DEBUG", logger="ske_bridge"):
        run(lambda: celery_app, echo=lines.append, minute=WEDNESDAY_1001)

    assert lines == []
    assert [record.getMessage() for record in caplog.records] == ["ske: nothing due"]


@pytest.mark.parametrize("missing", [
    NoCeleryApp("no Celery app is configured"),
    ModuleNotFoundError("No module named 'celery'", name="celery"),
])
def test_tasks_run_without_a_celery_app(missing):
    define("* * * * *", "native")

    def no_celery():
        raise missing

    result = run(no_celery, echo=lambda _: None, minute=WEDNESDAY_1001)

    assert (result.beat, result.tasks.ran, result.ok) == (None, ["native"], True)


def test_without_tasks_or_a_celery_app_the_run_fails_naming_both():
    def no_celery():
        raise NoCeleryApp("no Celery app is configured")

    result = run(no_celery, echo=lambda _: None, minute=WEDNESDAY_1001)

    assert not result.ok
    assert result.failure == (
        "no scheduled tasks are defined (is the module that defines them imported, e.g. from AppConfig.ready()?) "
        "and no Celery app is configured"
    )


def test_a_broken_celery_app_fails_the_run_but_the_tasks_still_run(caplog):
    define("* * * * *", "native")

    def broken():
        raise ModuleNotFoundError("No module named 'proj.celery'", name="proj.celery")

    lines = []
    with caplog.at_level("ERROR", logger="ske_bridge"):
        result = run(broken, echo=lines.append, minute=WEDNESDAY_1001)

    assert result.tasks.ran == ["native"]
    assert not result.ok
    assert result.failure == "the Celery beat schedule could not run: ModuleNotFoundError: No module named 'proj.celery'"
    assert lines[0] == "failed the beat schedule: ModuleNotFoundError: No module named 'proj.celery'"
    assert caplog.records[0].getMessage() == "ske: the Celery beat schedule could not run"


def test_a_broken_beat_schedule_fails_the_run_but_the_tasks_still_run(celery_app, monkeypatch):
    monkeypatch.setitem(celery_app.conf, "beat_schedule", {"bad": {"task": "tests.record", "schedule": "not a schedule"}})
    define("* * * * *", "native")

    result = run(lambda: celery_app, echo=lambda _: None, minute=WEDNESDAY_1001)

    assert result.tasks.ran == ["native"]
    assert not result.ok
    assert result.failure.startswith("the Celery beat schedule could not run: ")


def test_the_minute_is_the_one_the_platform_scheduled(monkeypatch):
    monkeypatch.setenv("SKE_SCHEDULED_AT", "2026-03-04T12:00:59+02:00")
    define("0 10 * * *", "ten-utc", time_zone="UTC")

    assert run(echo=lambda _: None).tasks.ran == ["ten-utc"]


# Jobs the tasks dispatch, off the platform


def test_off_the_platform_a_run_waits_for_the_jobs_its_tasks_dispatched_and_theirs():
    seen = []

    @job("child")
    def child(payload, ctx):
        time.sleep(0.05)
        seen.append(("child", payload))

    @job("parent")
    def parent(payload, ctx):
        seen.append(("parent", payload))
        child.dispatch(payload + 1, delay=1)

    define("* * * * *", "dispatches", lambda: parent.dispatch(1))

    result = run(echo=lambda _: None, minute=WEDNESDAY_1001)

    assert result.ok and result.jobs.ok
    assert seen == [("parent", 1), ("child", 2)]


def test_off_the_platform_a_failing_job_fails_the_run(caplog):
    @job("broken")
    def broken(payload, ctx):
        raise RuntimeError("the job broke")

    define("* * * * *", "dispatches", lambda: broken.dispatch())
    lines = []

    result = run(echo=lines.append, minute=WEDNESDAY_1001)

    assert not result.ok
    [label] = result.jobs.failed
    assert label.startswith("broken [")
    assert f"job {label} failed" in lines
    assert result.failure == "1 job(s) dispatched by the schedule did not finish"


def test_off_the_platform_a_job_due_after_the_wait_fails_the_run_at_once():
    @job("later")
    def later(payload, ctx):
        pass

    define("* * * * *", "dispatches", lambda: later.dispatch(delay=30))
    lines = []

    started = time.monotonic()
    result = run(echo=lines.append, minute=WEDNESDAY_1001, jobs_wait=5)

    assert time.monotonic() - started < 2
    [label] = result.jobs.unfinished
    assert label.startswith("later [")
    assert any(line.startswith(f"job {label} did not finish") for line in lines)
    assert not result.ok


def test_off_the_platform_a_job_still_running_when_the_wait_ends_fails_the_run():
    release = threading.Event()

    @job("stuck")
    def stuck(payload, ctx):
        release.wait(5)

    define("* * * * *", "dispatches", lambda: stuck.dispatch())

    try:
        result = run(echo=lambda _: None, minute=WEDNESDAY_1001, jobs_wait=0.2)
    finally:
        release.set()

    assert len(result.jobs.unfinished) == 1
    assert not result.ok


def test_jobs_dispatched_outside_a_run_are_not_waited_for():
    ran = threading.Event()

    @job("elsewhere")
    def elsewhere(payload, ctx):
        ran.wait(5)

    elsewhere.dispatch()
    try:
        result = run(echo=lambda _: None, minute=WEDNESDAY_1001, jobs_wait=1)
    finally:
        ran.set()

    assert result.jobs.ok


def test_on_the_platform_dispatched_jobs_go_to_the_queue_and_are_not_waited_for(platform, monkeypatch, jobs_sent):
    monkeypatch.setenv("SQS_PREFIX", "https://sqs.ca-central-1.amazonaws.com/123456789012")

    @job("queued")
    def queued(payload, ctx):
        raise AssertionError("runs on the queue, not here")

    define("* * * * *", "dispatches", lambda: queued.dispatch())

    result = run(echo=lambda _: None, minute=WEDNESDAY_1001)

    assert result.ok and result.jobs is None
    assert len(jobs_sent) == 1
