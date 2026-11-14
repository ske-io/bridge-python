"""Runs the job messages of a delivered batch (see ske_bridge.queue for the
batch and how jobs and Celery messages share it).

Each job record is run once, in order, and gets exactly one outcome, with
the queue's ``tries``, ``backoff`` and ``timeout``:

- An unreadable body, or one naming a job this process does not define,
  fails at once: running it again cannot help.
- A delivery already past ``tries`` (the total attempts allowed, 1 when
  unset) fails without running.
- The handler returned: deleted.
- The handler raised or ran out of time: released after ``backoff`` seconds
  while attempts are left, else failed.
- The batch's time ran out before the job started, or earlier work in the
  batch left it short of its full time and it ran out (see
  ske_bridge.queue.cut_by_batch): it is released at once, without using a
  try, to run in a later delivery, without a failure logged.

``attempts`` is the platform's count of the tries the job has used, this one
included. A release that uses a try (a failure or the job's own timeout,
released after ``backoff``) counts toward ``tries``; one for the batch's time
does not: its outcome says ``keeps_tries``, and the platform delivers the
message again with its count kept. Where the platform cannot do that, the
next delivery counts as a try after all.

Time: each job has ``timeout`` seconds (60 when unset) less a margin (5
seconds, or half the timeout when that is less) for reporting outcomes. The
batch has one deadline as well: the platform's ``deadline_ms``, less the same
kind of margin, or else the job time counted from when the batch arrived. A
job's limit is the earlier of the two. A first job runs to the batch's
deadline, a little before its own limit, and running out of time there is
its own timeout; a later job that earlier work left short of that time is
released.

Stopping a job: when it runs out of time its context's ``cancelled`` event is
set, and, where the bridge can, it is interrupted:

- A sync handler on the main thread (a sync WSGI worker, the way
  ``ske_bridge.wsgi:queue`` is served) gets JobTimeout raised in it by
  SIGALRM, and again each second it keeps running.
- An async handler runs in an event loop of its own and is cancelled at its
  limit; it has a short grace to finish. On the main thread, one that holds
  the event loop past that grace gets JobTimeout raised in it as a sync
  handler does.
- A sync handler on any other thread cannot be interrupted, and runs until it
  returns.

Whatever it returned with, a handler that ends past its limit is out of time
and never acknowledged.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import signal
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any

from ske_bridge import jobs
from ske_bridge.jobs import JobContext, JobTimeout
from ske_bridge.jobs.message import InvalidMessage, decode
from ske_bridge.outcome import JobOutcome
from ske_bridge.queue import MAX_RELEASE_DELAY, batch_deadline, cut_by_batch, deadline_ms, job_timeout

logger = logging.getLogger("ske_bridge")

#: The queue's timeout when the platform does not give one.
DEFAULT_TIMEOUT = 60

#: How long, in seconds, a handler that ran out of time is given to stop, at
#: most: an async handler after it is cancelled, and a sync one between
#: interruptions.
GRACE = 1.0

#: How long past an async handler's grace the main thread's alarm comes, in
#: seconds, so the event loop's own cancellation is never beaten to it.
ALARM_AFTER_GRACE = 0.1

@dataclass(frozen=True)
class Policy:
    """A batch's settings for its jobs; times on the ``time.monotonic()``
    clock."""

    tries: int
    backoff: int
    #: The seconds one job may run.
    job_seconds: float
    #: When the batch's time for starting and running jobs ends.
    cutoff: float
    #: When the platform ends the request.
    end: float
    #: The seconds from the batch's arrival to its cutoff.
    whole: float


class JobRunner:
    def __init__(self, around_job: Callable[[], AbstractContextManager[Any]]):
        self.around_job = around_job

    def policy(self, options: dict[str, Any], received: float) -> Policy:
        """The batch's policy from its options. ``received`` is when the
        batch arrived (``time.monotonic()``)."""
        timeout = _positive_int(options.get("timeout")) or DEFAULT_TIMEOUT
        tries = _positive_int(options.get("tries")) or 1
        backoff = min(_non_negative_int(options.get("backoff")) or 0, MAX_RELEASE_DELAY)
        job_seconds = job_timeout(timeout)

        platform_ms = deadline_ms(options)
        cutoff = batch_deadline({"deadline_ms": platform_ms} if platform_ms else {"timeout": timeout}, received)
        end = time.monotonic() + (platform_ms / 1000 - time.time()) if platform_ms else received + timeout
        return Policy(tries=tries, backoff=backoff, job_seconds=job_seconds, cutoff=cutoff, end=end, whole=cutoff - received)

    def not_a_job(self, record: dict[str, Any], reason: BaseException | None) -> JobOutcome:
        """The outcome for a message that is not a job message, in an
        application without Celery to run it."""
        message_id = _message_id(record)
        why = f" and no Celery app runs it ({type(reason).__name__}: {reason})" if reason else ""
        logger.error("ske: message %s is not a job%s", message_id, why)
        return JobOutcome.failure(message_id, f"unreadable message: not a job message{why}", attempts=_positive_int(record.get("attempts")) or 1)

    def run(self, record: dict[str, Any], policy: Policy) -> JobOutcome:
        message_id = _message_id(record)
        attempts = _positive_int(record.get("attempts")) or 1

        try:
            message = decode(record.get("body"))
        except InvalidMessage as e:
            logger.error("ske: message %s is not a job: %s", message_id, e)
            return JobOutcome.failure(message_id, f"unreadable message: {e}", attempts=attempts)

        defined = jobs.get(message.job)
        if defined is None:
            logger.error("ske: message %s names job %s, which is not defined", message_id, message.job)
            return JobOutcome.failure(message_id, f"job {message.job!r} is not defined", attempts=attempts)

        if attempts > policy.tries:
            return JobOutcome.failure(message_id, f"delivered {attempts} times, more than the {policy.tries} tries allowed", attempts=attempts)

        started = time.monotonic()
        left = policy.cutoff - started
        if left <= 0:
            return JobOutcome.release_for_time(message_id, attempts=attempts, error="not started: the batch's time ran out")
        cut = cut_by_batch(left, policy.job_seconds, policy.whole)
        limit = min(left, policy.job_seconds)
        stop_at = started + limit
        label = f"job {message.job} [{message.id}]"
        # A cancelled async handler's grace ends well before the platform's.
        grace = max(0.0, min(GRACE, (policy.end - stop_at) / 2))

        ctx = JobContext(message.id, message.job, attempts, deadline=stop_at)
        timed_out, error = False, None
        try:
            with self.around_job():
                _run_handler(defined.handler, message.payload, ctx, limit, grace)
        except JobTimeout:
            timed_out = True
        except Exception as e:
            error = e

        # A handler that cannot be interrupted, or that held the event loop,
        # can end past its limit however it ended: it is out of time.
        if timed_out or time.monotonic() > stop_at:
            ctx.cancelled.set()
            if error is not None:
                logger.error("ske: %s failed after it ran out of time", label, exc_info=error)
            if cut:
                logger.warning("ske: %s was stopped at the batch's deadline; it runs again in a later delivery", label)
                return JobOutcome.release_for_time(message_id, attempts=attempts, error="stopped at the batch's deadline")
            return self._retry_or_fail(message_id, attempts, policy, f"{label} ran past its {round(limit)}s time limit")

        if error is not None:
            logger.error("ske: %s failed", label, exc_info=error)
            return self._retry_or_fail(message_id, attempts, policy, _describe(error))

        return JobOutcome.success(message_id, attempts=attempts)

    @staticmethod
    def _retry_or_fail(message_id: str, attempts: int, policy: Policy, error: str) -> JobOutcome:
        if attempts < policy.tries:
            return JobOutcome.release(message_id, delay=policy.backoff, attempts=attempts, error=error)
        return JobOutcome.failure(message_id, error, attempts=attempts)


def _run_handler(handler: Callable[..., Any], payload: Any, ctx: JobContext, limit: float, grace: float) -> None:
    """Runs a handler within ``limit`` seconds; raises JobTimeout when it was
    stopped for running out of time, or what the handler raised."""
    if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(getattr(handler, "__call__", None)):
        _run_async(handler(payload, ctx), ctx, limit, grace)
        return

    with _SyncLimit(limit, ctx.cancelled):
        result = handler(payload, ctx)
    if inspect.isawaitable(result):
        # A sync function that returned a coroutine: it runs for what is left.
        _run_async(result, ctx, ctx.time_left() or 0.0, grace)


def _run_async(awaitable: Any, ctx: JobContext, limit: float, grace: float) -> None:
    """Runs an awaitable in an event loop of its own, in a thread of its own
    when this thread already runs one; at ``limit`` it is cancelled and given
    ``grace`` to finish, then left behind."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # Cancellation needs the loop back. On the main thread SIGALRM is the
        # backstop for a handler that holds it: it comes once the grace has
        # passed too, so a handler that yields is cancelled first.
        with _SyncLimit(limit + grace + ALARM_AFTER_GRACE, ctx.cancelled, set_cancelled=False):
            _in_new_loop(awaitable, ctx, limit, grace)
        return

    failure: list[BaseException] = []
    context = contextvars.copy_context()

    def in_thread() -> None:
        try:
            context.run(_in_new_loop, awaitable, ctx, limit, grace)
        except BaseException as e:
            failure.append(e)

    thread = threading.Thread(target=in_thread, name=f"ske-job-{ctx.name}")
    thread.start()
    thread.join()
    if failure:
        raise failure[0]


def _in_new_loop(awaitable: Any, ctx: JobContext, limit: float, grace: float) -> None:
    loop = asyncio.new_event_loop()
    timed_out = False
    try:
        async def body() -> Any:
            return await awaitable

        task = loop.create_task(body())
        try:
            loop.run_until_complete(asyncio.wait({task}, timeout=limit))
        except JobTimeout:
            # The alarm came while the loop itself ran, not the handler.
            timed_out = True
            raise
        if task.done() and isinstance(task.exception() if not task.cancelled() else None, JobTimeout):
            # The alarm interrupted the handler holding the loop.
            timed_out = True
        if not task.done():
            timed_out = True
            ctx.cancelled.set()
            task.cancel()
            loop.run_until_complete(asyncio.wait({task}, timeout=grace))
            if not task.done():
                logger.error("ske: job %s [%s] ignored its cancellation; it is left behind", ctx.name, ctx.id)
            elif not task.cancelled() and task.exception() is not None:
                logger.error("ske: job %s [%s] failed after it ran out of time", ctx.name, ctx.id, exc_info=task.exception())
            raise JobTimeout(f"the job ran past its {limit:g}s time limit")
        task.result()
    finally:
        # A job out of time is not waited for: neither a task that ignored
        # its cancellation nor a thread it started (asyncio.to_thread) stops
        # when told.
        if not timed_out:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()


class _SyncLimit:
    """A sync handler's time limit. On the main thread, where SIGALRM can
    interrupt it, JobTimeout is raised in it at the limit and again each
    GRACE seconds it keeps running; elsewhere ``cancelled`` is set at the
    limit and the handler runs on.

    The alarm can come just after the handler returned, before the limit is
    disarmed: the JobTimeout it raises then is right, as the handler ended
    past its limit. One that comes while it is disarmed does nothing."""

    def __init__(self, seconds: float, cancelled: threading.Event, set_cancelled: bool = True):
        self.seconds = seconds
        self.cancelled = cancelled
        #: Off the main thread, whether to set ``cancelled`` at the limit:
        #: an async handler's own loop does that.
        self.set_cancelled = set_cancelled
        self._armed = False
        self._previous: Any = None
        self._timer: threading.Timer | None = None

    def __enter__(self) -> _SyncLimit:
        if hasattr(signal, "setitimer") and threading.current_thread() is threading.main_thread():
            previous = signal.getsignal(signal.SIGALRM)
            if previous is not None:
                self._previous = previous
                self._armed = True
                signal.signal(signal.SIGALRM, self._on_alarm)
                signal.setitimer(signal.ITIMER_REAL, self.seconds, GRACE)
                return self
            # A handler installed from C cannot be put back from Python.
            logger.warning("ske: SIGALRM has a handler Python did not install; jobs are not interrupted")
        if self.set_cancelled:
            self._timer = threading.Timer(self.seconds, self.cancelled.set)
            self._timer.daemon = True
            self._timer.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._armed:
            signal.setitimer(signal.ITIMER_REAL, 0)
            self._armed = False
            signal.signal(signal.SIGALRM, self._previous)
        if self._timer is not None:
            self._timer.cancel()

    def _on_alarm(self, signum: int, frame: Any) -> None:
        # Once in __exit__ the handler has returned, and the limit must be
        # disarmed in full; the runner sees the time it ended at.
        if not self._armed or frame.f_code is _SyncLimit.__exit__.__code__:
            return
        self.cancelled.set()
        raise JobTimeout(f"the job ran past its {self.seconds:g}s time limit")


def _message_id(record: dict[str, Any]) -> str:
    value = record.get("message_id")
    return value if isinstance(value, str) else str(value if value is not None else "")


def _describe(error: BaseException) -> str:
    text = str(error)
    return f"{type(error).__name__}: {text}" if text else type(error).__name__


def _positive_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _non_negative_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None
