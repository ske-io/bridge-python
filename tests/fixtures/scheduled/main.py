"""An app that is neither Django nor Flask, with scheduled tasks and a job,
for ``ske-bridge schedule``. Environment variables make tasks fail or
dispatch jobs."""

import asyncio
import os

from ske_bridge import schedule
from ske_bridge.jobs import job


@job("child")
def child(payload, ctx):
    print(f"child ran {payload}", flush=True)
    if os.environ.get("FAIL_JOB") and payload == 1:
        raise RuntimeError("the job broke")


@job("parent")
def parent(payload, ctx):
    print("parent ran", flush=True)
    child.dispatch(1)


@schedule.every_minute(name="every-minute")
def every_minute():
    pass


@schedule.cron("15 * * * *", name="quarter-past")
def quarter_past():
    pass


@schedule.hourly(name="hourly")
def hourly():
    pass


@schedule.daily(at="09:00", time_zone="Europe/Paris", name="paris-nine")
def paris_nine():
    pass


@schedule.cron("*/30 * * * * *", name="every-thirty-seconds")
def every_thirty_seconds():
    pass


@schedule.every_minute(name="runs-in-maintenance", even_in_maintenance=True)
def runs_in_maintenance():
    pass


@schedule.every_minute(name="slow")
async def slow():
    await asyncio.sleep(0.1)
    print("slow finished", flush=True)


@schedule.every_minute(name="fails-when-asked")
def fails_when_asked():
    if os.environ.get("FAIL_TASK"):
        raise RuntimeError("the task broke")


@schedule.every_minute(name="dispatches")
def dispatches():
    if os.environ.get("DISPATCH"):
        parent.dispatch()
    if os.environ.get("DELAYED_JOB"):
        child.dispatch(2, delay=1)
    if os.environ.get("LATE_JOB"):
        child.dispatch(3, delay=900)


def app(environ, start_response):
    start_response("200 OK", [])
    return [b"ok"]
