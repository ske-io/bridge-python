"""Runs the messages the platform delivered in one call to ``/__ske/queue``.

The platform posts a batch of queue records and the queue's settings::

    {"source": "sqs",
     "records": [{"message_id": "…", "body": "…", "attempts": 1}],
     "options": {"timeout": 60, "tries": 3, "backoff": 5, "deadline_ms": 1800000000000}}

One queue can carry both kinds of message the bridge runs, so a batch may mix
them, and each record is told apart by its body alone:

- A job message (ske_bridge.jobs) is a JSON object with a ``"job"`` key. It
  runs through ske_bridge.jobs.runner, with the queue's ``tries``,
  ``backoff`` and ``timeout``.
- Anything else goes to Celery, when the application uses it. Celery's
  envelope is never a JSON object with a ``"job"`` key, base64-encoded or
  not.

Every record gets exactly one outcome (ske_bridge.outcome), in the order the
records arrived, and the batch has one deadline for both kinds.

When the application cannot run a message at all, nothing of the batch runs
and the queue route answers 500, so the platform delivers it again once the
deployment is fixed: an application that defines no job and has no Celery
app, or a Celery message in an application whose Celery app cannot be found.
A message that is neither a job nor a Celery message, in an application that
defines jobs, is a dead letter instead, as a job message naming no defined
job is: delivering it again cannot help.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from ske_bridge.outcome import JobOutcome

#: Seconds kept between the job timeout and the queue's timeout, for stopping
#: a job that ran out of time and answering the platform.
TIMEOUT_MARGIN = 5

#: The longest SQS hides a released message: 12 hours.
MAX_RELEASE_DELAY = 43200


class NoQueueRunner(LookupError):
    """Nothing in this app runs queued jobs."""


NO_RUNNER = (
    "nothing runs queued jobs in this app: set SKE_CELERY_APP to its Celery app as celery -A names it "
    "(for example proj.celery:app), or define jobs with ske_bridge.jobs"
)


def job_timeout(queue_timeout: int) -> int:
    """The longest a batch may run on a queue with the given timeout."""
    return max(1, queue_timeout - min(TIMEOUT_MARGIN, queue_timeout // 2))


def deadline_ms(options: dict[str, Any]) -> float | None:
    """The platform's ``deadline_ms``, when it is a positive number."""
    value = options.get("deadline_ms")
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
        return float(value)
    return None


def batch_deadline(options: dict[str, Any], received: float | None = None) -> float | None:
    """When the batch must stop, on the ``time.monotonic()`` clock, or None
    when the platform gives no limit.

    ``deadline_ms`` is when the platform cuts the batch off, in Unix epoch
    milliseconds; the margin is kept back from the time that was left until
    then when the batch arrived, as job_timeout() keeps it from a timeout.
    Without it the queue's ``timeout`` counts from ``received``
    (``time.monotonic()``), when the batch arrived, by default now.
    """
    now = time.monotonic()
    received = now if received is None else received
    cutoff_ms = deadline_ms(options)
    if cutoff_ms is not None:
        left = cutoff_ms / 1000 - time.time()
        at_arrival = left + (now - received)
        return now + left - min(TIMEOUT_MARGIN, max(at_arrival, 0) / 2)

    timeout = options.get("timeout")
    if isinstance(timeout, int) and not isinstance(timeout, bool) and timeout > 0:
        return received + job_timeout(timeout)
    return None


#: The most of a job's full time that earlier work in its batch may take with
#: the job still owning a timeout, in seconds (see cut_by_batch).
MAX_CUT_SLACK = 1.0


def cut_by_batch(left: float, own: float | None, whole: float) -> bool:
    """Whether the batch's deadline, ``left`` seconds away as a job starts,
    cuts the job short of its full time because earlier work in the batch
    used that time up.

    A job's full time is what it gets as the batch's first: its own limit
    ``own`` (None when it has none), or the batch's ``whole`` time (from when
    the batch arrived to its deadline) when that is shorter. The platform's
    deadline comes from its own timeout, which is the queue's, less a margin
    it keeps for answering, and reaching the bridge takes a moment more; so
    on the platform the whole batch is a little shorter than a job's own
    limit, and a first job runs to the batch's deadline. Running into it is
    still the job's own timeout, which uses a try.

    A later job is cut only when the time before it took more than a tenth
    of its full time, a second at most: the same allowance the platform keeps
    back from a batch's time for answering. Less than that is the bridge's
    own work between records (reading the batch, decoding a message), not
    another job's run, and a job that had all but that much of its full time
    ran out of time on its own.
    """
    full = whole if own is None else min(own, whole)
    return left < full - min(MAX_CUT_SLACK, max(full, 0) / 10)


class QueueRunner:
    """Runs a batch of job messages and Celery messages.

    ``celery`` builds the Celery runner (ske_bridge.celery.runner), the first
    time a batch holds a message that is not a job; it raises when the
    application has no Celery app it can run (NoQueueRunner, NoCeleryApp,
    or an ImportError without Celery installed), and is asked again on the
    next batch that needs it. ``around_job`` wraps each job's run, as it
    wraps each Celery task's: a Flask app context, or Django's database
    connection upkeep.
    """

    def __init__(
        self,
        celery: Callable[[], Any] | None = None,
        around_job: Callable[[], AbstractContextManager[Any]] | None = None,
    ):
        from ske_bridge.jobs.runner import JobRunner

        self._make_celery = celery
        self._celery: Any = None
        self.jobs = JobRunner(around_job or nullcontext)

    @property
    def celery(self) -> Any:
        """The Celery runner, once a batch needed it."""
        return self._celery

    def process(self, records: list[Any], options: dict[str, Any] | None = None, received: float | None = None) -> list[JobOutcome]:
        """One outcome per record, in the order they arrived. ``received`` is
        when the batch's request arrived (``time.monotonic()``, now by
        default): reading it, and building the Celery runner, count against
        the batch's time."""
        from ske_bridge import jobs
        from ske_bridge.jobs.message import is_job_message

        received = time.monotonic() if received is None else received
        options = options or {}
        records = [r if isinstance(r, dict) else {} for r in records]
        kinds = [is_job_message(r.get("body")) for r in records]

        # An app that defines no job runs only Celery, which must be there;
        # one that does needs Celery only for messages that are not jobs.
        celery, no_celery = None, None
        if not jobs.defined() or not all(kinds):
            try:
                celery = self._celery_runner()
            except (LookupError, ImportError) as e:
                if not jobs.defined() or any(_looks_like_celery(r.get("body")) for r, job in zip(records, kinds) if not job):
                    raise
                no_celery = e

        deadline = batch_deadline(options, received)
        policy = self.jobs.policy(options, received)

        outcomes = []
        for record, job in zip(records, kinds):
            if job:
                outcomes.append(self.jobs.run(record, policy))
            elif celery is not None:
                outcomes.append(celery.run(record, deadline, received))
            else:
                outcomes.append(self.jobs.not_a_job(record, no_celery))
        return outcomes

    def _celery_runner(self) -> Any:
        if self._celery is None:
            if self._make_celery is None:
                raise NoQueueRunner(NO_RUNNER)
            self._celery = self._make_celery()
        return self._celery


def _looks_like_celery(body: Any) -> bool:
    """Whether a body is a Celery task message envelope, so only a Celery
    app could run it."""
    from ske_bridge.celery.message import InvalidMessage, _envelope

    try:
        headers = _envelope(body).get("headers")
    except InvalidMessage:
        return False
    return isinstance(headers, dict) and "task" in headers
