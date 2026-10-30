"""The Celery app the tests run tasks from. Its broker is SQS, which
conftest.py stubs, so tasks are published exactly as kombu's SQS transport
publishes them and never leave the process."""

import time

from celery import Celery
from celery.exceptions import Ignore, Reject
from celery.schedules import crontab

QUEUE_URL = "https://sqs.ca-central-1.amazonaws.com/123456789012/default--shop-production"

app = Celery("tests", set_as_current=False)
app.conf.update(
    broker_url="sqs://",
    broker_transport_options={
        "region": "ca-central-1",
        "predefined_queues": {"default": {"url": QUEUE_URL}},
    },
    task_default_queue="default",
    timezone="Europe/Paris",
    beat_schedule={
        "every-minute": {"task": "tests.record", "schedule": 60.0, "args": ("minute",)},
        "every-five": {"task": "tests.record", "schedule": 300, "args": ("five",)},
        "weekday-mornings": {
            "task": "tests.record",
            "schedule": crontab(minute=30, hour=9, day_of_week="mon-fri"),
            "kwargs": {"value": "morning"},
            "options": {"queue": "default"},
        },
    },
)

#: What the tasks were called with, in order.
calls: list = []


@app.task(name="tests.add")
def add(a, b):
    calls.append(("add", a, b))
    return a + b


@app.task(name="tests.record")
def record(value=None):
    calls.append(("record", value))
    return value


@app.task(name="tests.fail")
def fail():
    raise RuntimeError("it broke")


@app.task(name="tests.retry", bind=True, max_retries=2, default_retry_delay=30)
def retry(self):
    calls.append(("retry", self.request.retries))
    raise self.retry(exc=ConnectionError("upstream away"))


@app.task(name="tests.autoretry", autoretry_for=(ConnectionError,), retry_kwargs={"countdown": 7, "max_retries": 3})
def autoretry():
    raise ConnectionError("try later")


@app.task(name="tests.reject", bind=True)
def reject(self, requeue):
    raise Reject("not now", requeue=requeue)


@app.task(name="tests.ignore")
def ignore():
    raise Ignore()


@app.task(name="tests.slow")
def slow(seconds):
    time.sleep(seconds)
    return "finished"


@app.task(name="tests.soft", soft_time_limit=0.2)
def soft():
    time.sleep(2)


@app.task(name="tests.retry_args", bind=True, max_retries=5)
def retry_args(self, n):
    calls.append(("retry_args", n, self.request.retries))
    raise self.retry(args=(n + 1,), countdown=10)


@app.task(name="tests.replace", bind=True)
def replace(self):
    raise self.replace(add.s(20, 22))


@app.task(name="tests.soft_then_hard", soft_time_limit=0.2, time_limit=0.5)
def soft_then_hard():
    from celery.exceptions import SoftTimeLimitExceeded

    try:
        time.sleep(2)
    except SoftTimeLimitExceeded:
        calls.append(("soft",))
        time.sleep(2)
