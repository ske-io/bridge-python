"""Runs the Celery messages the platform delivered in one invocation.

Each message is run in this process, through Celery's own tracer, the code a
worker runs a task with: signals, ``before_start``/``on_success``/
``on_failure``, result backend updates, links, chains and ``replace()`` all
behave as they do under ``celery worker``. Nothing polls the broker; the
platform pushes each message, and the outcome says what becomes of it:

- The task returned (or raised ``Ignore``): the message is acknowledged.
- The task called ``self.retry()`` (or retried through ``autoretry_for``):
  as under a worker, the retry publishes a new message carrying the retry's
  arguments, its eta and ``retries + 1``, and the delivered one is
  acknowledged. When that publish fails, the delivered message is released
  instead, so the platform delivers it again rather than lose the task.
- The task raised anything else, or ran out of retries: it failed for good,
  and the message is a dead letter for the platform to keep (see
  JobOutcome). As under a worker, an exception is not retried unless the task
  asks for it.
- ``Reject(requeue=True)`` releases the message; ``Reject`` without requeue
  fails it.
- A message whose ``eta`` (``countdown``) is still ahead is released, unrun,
  until then or for the longest the platform can hold a message (12 hours),
  whichever comes first; its eta is checked again on the next delivery. One
  whose ``expires`` has passed is revoked unrun, as a worker discards it.

The retry count is the message's own ``retries`` header, which each retry's
new message carries, never the platform's delivery count: a release for an
eta, or a redelivery after a crash, is not a retry.

Time limits work as a worker's: at the soft limit the task gets
``SoftTimeLimitExceeded``, and at the hard limit ``TimeLimitExceeded``. The
batch also has one deadline, less a margin for answering: the time the
platform says it cuts the batch off (``options.deadline_ms``), or else the
queue's timeout counted from when the batch arrived. Each task's hard limit is
cut to the time left. A task stopped by the batch's deadline rather than its
own limit did nothing wrong, so it is released (as ``Reject(requeue=True)``,
which runs no failure handling) and delivered again in a later invocation;
a message that has not started when too little is left is released too.
Interrupting a task needs ``SIGALRM`` and the main thread, which a
gunicorn sync worker serves requests on; elsewhere the platform's own timeout
is the only limit.
"""

from __future__ import annotations

import logging
import math
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from typing import Any

from ske_bridge.celery.message import InvalidMessage, TaskMessage, decode
from ske_bridge.celery.outcome import JobOutcome

logger = logging.getLogger("ske_bridge")

#: Seconds kept between the job timeout and the queue's timeout, for failing a
#: task that ran out of time and answering the platform.
TIMEOUT_MARGIN = 5

#: The fewest seconds a task is started with. A message reached with less of
#: the batch's time left is released for another invocation.
MIN_START = 0.5

#: The longest SQS hides a released message: 12 hours.
MAX_RELEASE_DELAY = 43200


def job_timeout(queue_timeout: int) -> int:
    """The longest a batch may run on a queue with the given timeout."""
    return max(1, queue_timeout - min(TIMEOUT_MARGIN, queue_timeout // 2))


def batch_deadline(options: dict[str, Any]) -> float | None:
    """When the batch must stop, on the ``time.monotonic()`` clock, or None
    when the platform gives no limit.

    ``deadline_ms`` is when the platform cuts the batch off, in Unix epoch
    milliseconds; the margin is kept back from the time left until then, as
    job_timeout() keeps it from a timeout. Without it the queue's ``timeout``
    counts from now, when the batch arrived.
    """
    deadline_ms = options.get("deadline_ms")
    if isinstance(deadline_ms, (int, float)) and not isinstance(deadline_ms, bool) and deadline_ms > 0:
        left = deadline_ms / 1000 - time.time()
        return time.monotonic() + left - min(TIMEOUT_MARGIN, max(left, 0) / 2)

    timeout = options.get("timeout")
    if isinstance(timeout, int) and not isinstance(timeout, bool) and timeout > 0:
        return time.monotonic() + job_timeout(timeout)
    return None


class QueueRunner:
    """``around_job`` wraps each task's run: a Flask app context, or Django's
    database connection upkeep."""

    def __init__(self, app: Any, around_job: Callable[[], AbstractContextManager[Any]] | None = None):
        from kombu.serialization import prepare_accept_content

        self.app = app
        self.around_job = around_job or nullcontext
        self.accept = prepare_accept_content(app.conf.accept_content)
        self.hostname = socket.gethostname()
        self._tracers: dict[str, Callable[..., Any]] = {}
        self._run: _Run | None = None

    def process(self, records: list[Any], options: dict[str, Any] | None = None) -> list[JobOutcome]:
        """One outcome per record, in the order they arrived."""
        deadline = batch_deadline(options or {})

        return [self._process_one(r if isinstance(r, dict) else {}, deadline) for r in records]

    def _process_one(self, record: dict[str, Any], deadline: float | None) -> JobOutcome:
        message_id = str(record.get("message_id") or "")
        deliveries = _int(record.get("attempts"))

        try:
            message = decode(str(record.get("body") or ""), self.accept)
        except InvalidMessage as e:
            return JobOutcome.failure(message_id, f"unreadable message: {e}", attempts=deliveries or 1)

        attempts = message.retries + 1

        task = self.app.tasks.get(message.task)
        if task is None:
            return JobOutcome.failure(
                message_id,
                f"task {message.task!r} is not registered in the Celery app {self.app.main!r}",
                attempts=attempts,
            )

        request = self._request(message, deliveries)

        now = datetime.now(UTC)
        if message.expires is not None and message.expires <= now:
            self._revoke(task, message, request)
            return JobOutcome.success(message_id, attempts=attempts, error=f"expired at {message.expires.isoformat()}, revoked unrun")
        if message.eta is not None and message.eta > now:
            delay = min(math.ceil((message.eta - now).total_seconds()), MAX_RELEASE_DELAY)
            return JobOutcome.release(message_id, delay=delay)

        hard, soft = self._limits(task, message)
        cut = False
        if deadline is not None:
            left = deadline - time.monotonic()
            if left < MIN_START:
                return JobOutcome.release(message_id, error="not started: the batch's time ran out")
            if not hard or left < hard:
                hard, cut = left, True
        if soft is not None and hard is not None and soft >= hard:
            soft = None

        run = self._run = _Run(soft, hard, cut=cut)
        try:
            with self.around_job():
                _, info, _, _ = self._tracer(task)(message.id, message.args, message.kwargs, request)
        except Exception as e:
            if run.returned:
                # The task ran to its end; storing its result or sending its
                # links failed after. Running it again would repeat it.
                logger.exception("ske: task %s[%s] succeeded, but its result could not be saved", message.task, message.id)
                return JobOutcome.success(message_id, attempts=attempts, error=f"succeeded, but saving its result failed: {_describe(e)}")
            # The task did not finish: delivering it again is the only way it
            # still runs.
            logger.exception("ske: task %s[%s] could not be run", message.task, message.id)
            return JobOutcome.release(message_id, attempts=attempts, error=_describe(e))
        finally:
            self._run = None

        return self._outcome(message_id, attempts, info)

    def _outcome(self, message_id: str, attempts: int, info: Any) -> JobOutcome:
        from celery import states

        if info is None or info.state == states.IGNORED:
            return JobOutcome.success(message_id, attempts=attempts)

        exc = info.retval
        if info.state == states.RETRY:
            # The retry published its own message; this one is done.
            return JobOutcome.success(message_id, attempts=attempts, error=f"retried: {_describe(exc)}")
        if info.state == states.REJECTED:
            if getattr(exc, "requeue", False):
                return JobOutcome.release(message_id, attempts=attempts, error=_describe(exc))
            if _raised_by_retry(exc):
                return JobOutcome.release(message_id, attempts=attempts, error=f"the retry could not be sent: {_describe(exc.reason)}")

        return JobOutcome.failure(message_id, _describe(exc), attempts=attempts)

    def _request(self, message: TaskMessage, deliveries: int) -> dict[str, Any]:
        """The request a worker builds from a message: its headers, delivery
        info and embedded callbacks.

        The delivery info is the message's own, as Celery published it: a
        retry or replacement is routed by its routing key, the application's
        name for the queue, not the platform's.
        """
        properties = message.properties
        delivery_info = dict(properties.get("delivery_info") or {})
        delivery_info["redelivered"] = deliveries > 1

        request = {
            **message.headers,
            "id": message.id,
            "task": message.task,
            "retries": message.retries,
            "delivery_info": delivery_info,
            "reply_to": properties.get("reply_to"),
            "correlation_id": properties.get("correlation_id") or message.id,
            "hostname": self.hostname,
            "is_eager": False,
            "args": message.args,
            "kwargs": message.kwargs,
        }
        request.update(message.embed)
        return request

    def _revoke(self, task: Any, message: TaskMessage, request: dict[str, Any]) -> None:
        """Records an expired task as revoked and announces it, as a worker
        does when it discards one."""
        from celery import signals
        from celery.app.task import Context

        logger.warning("ske: task %s[%s] expired at %s, revoked", message.task, message.id, message.expires.isoformat())
        context = Context(request)
        try:
            task.backend.mark_as_revoked(message.id, "expired", request=context, store_result=True)
        except Exception:
            logger.exception("ske: task %s[%s] could not be marked revoked", message.task, message.id)
        signals.task_revoked.send(sender=task, request=context, terminated=False, signum=None, expired=True)

    def _limits(self, task: Any, message: TaskMessage) -> tuple[float | None, float | None]:
        """``(hard, soft)``: the message's limits, else the task's, else the
        app's, as a worker takes them."""
        conf = self.app.conf
        header_hard, header_soft = message.time_limits
        hard = header_hard or task.time_limit or conf.task_time_limit
        soft = header_soft or task.soft_time_limit or conf.task_soft_time_limit
        return (float(hard) if hard else None), (float(soft) if soft else None)

    def _tracer(self, task: Any) -> Callable[..., Any]:
        """Celery's tracer for the task, with the call of the task's body
        wrapped so the time limits cover the body alone and the runner knows
        when it returned. The tracer reads the body into its ``fun``
        variable once; the wrapper replaces it there, so the application's
        task object is left untouched."""
        from celery.app.trace import build_tracer

        if task.name not in self._tracers:
            tracer = build_tracer(task.name, task, app=self.app, hostname=self.hostname)
            names = tracer.__code__.co_freevars
            if "fun" in names:
                cell = tracer.__closure__[names.index("fun")]
                cell.cell_contents = self._timed(cell.cell_contents)
            else:
                logger.warning("ske: this Celery version's tracer cannot be wrapped; time limits are not applied")
            self._tracers[task.name] = tracer
        return self._tracers[task.name]

    def _timed(self, fun: Callable[..., Any]) -> Callable[..., Any]:
        def body(*args: Any, **kwargs: Any) -> Any:
            run = self._run
            if run is None:
                return fun(*args, **kwargs)
            run.start(sys._getframe())
            try:
                result = fun(*args, **kwargs)
                run.returned = True
            finally:
                run.stop()
            return result

        return body


class _Run:
    """One task's time limits, enforced with SIGALRM while its body runs.

    The handler raises only while the body is running: when the alarm comes
    as the body has just returned (the interrupted frame is the wrapper's own,
    or stop()'s), it does nothing, so a task that finished is never failed by
    its limit.
    """

    def __init__(self, soft: float | None, hard: float | None, cut: bool = False):
        self.soft = soft
        self.hard = hard
        #: Whether the hard limit is the batch's deadline rather than the
        #: task's own.
        self.cut = cut
        self.returned = False
        self._frame: Any = None
        self._armed = False
        self._previous: Any = None
        self._phase_soft = False

    def start(self, frame: Any) -> None:
        first = self.soft or self.hard
        if not first or not hasattr(signal, "setitimer") or threading.current_thread() is not threading.main_thread():
            return

        previous = signal.getsignal(signal.SIGALRM)
        if previous is None:
            # A handler installed from C cannot be put back from Python, so
            # the limits are left to the platform rather than lose it.
            logger.warning("ske: SIGALRM has a handler Python did not install; time limits are not applied")
            return

        self._frame = frame
        self._previous = previous
        self._phase_soft = self.soft is not None
        self._armed = True
        signal.signal(signal.SIGALRM, self._on_alarm)
        signal.setitimer(signal.ITIMER_REAL, first)

    def stop(self) -> None:
        if not self._armed:
            return
        self._armed = False
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, self._previous)

    def _on_alarm(self, signum: int, frame: Any) -> None:
        from celery.exceptions import Reject, SoftTimeLimitExceeded, TimeLimitExceeded

        if not self._armed or frame is self._frame or frame.f_code is _Run.stop.__code__:
            return

        if self._phase_soft:
            self._phase_soft = False
            if self.hard:
                signal.setitimer(signal.ITIMER_REAL, max(self.hard - self.soft, 0.001))
            raise SoftTimeLimitExceeded(f"the task ran past its {self.soft:g}s soft time limit")
        if self.cut:
            raise Reject("stopped at the batch's deadline, to run again in a later delivery", requeue=True)
        raise TimeLimitExceeded(f"the task ran past its {self.hard:g}s time limit")


def _raised_by_retry(exc: Any) -> bool:
    """Whether a Reject is the one ``Task.retry()`` raises when it cannot
    publish the retry's message, rather than one the task raised itself."""
    from celery.app.task import Task

    tb = getattr(exc, "__traceback__", None)
    while tb is not None and tb.tb_next is not None:
        tb = tb.tb_next
    return tb is not None and tb.tb_frame.f_code is Task.retry.__code__


def _describe(exc: Any) -> str:
    if isinstance(exc, BaseException):
        text = str(exc)
        return f"{type(exc).__name__}: {text}" if text else type(exc).__name__
    return str(exc)


def _int(value: Any) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0
