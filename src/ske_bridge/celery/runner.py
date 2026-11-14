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
  instead, at once, so the platform delivers it again rather than lose the
  task.
- The task raised anything else, or ran out of retries: it failed for good,
  and the message is a dead letter for the platform to keep (see
  JobOutcome). As under a worker, an exception is not retried unless the task
  asks for it.
- ``Reject(requeue=True)`` releases the message at once; ``Reject`` without
  requeue fails it.
- A message whose ``eta`` (``countdown``) is still ahead is released, unrun,
  until then or for the longest the platform can hold a message (12 hours),
  whichever comes first; its eta is checked again on the next delivery. One
  whose ``expires`` has passed is revoked unrun, as a worker discards it.

The retry count is the message's own ``retries`` header, which each retry's
new message carries, never the platform's delivery count: a release for an
eta, or a redelivery after a crash, is not a retry.

Time limits are a worker's: at the soft limit the task gets
``SoftTimeLimitExceeded``, and at the hard limit ``TimeLimitExceeded``.
Unlike a worker, which kills its child process at the hard limit, the bridge
raises ``TimeLimitExceeded`` in the task's body, so ``autoretry_for`` or an
``except`` there can catch it. The
batch also has one deadline, less a margin for answering: the time the
platform says it cuts the batch off (``options.deadline_ms``), or else the
queue's timeout counted from when the batch arrived. Each task's hard limit is
cut to the time left. A task that earlier work in the batch left short of its
full time (its own hard limit, or the whole batch's time when it has none or
that is shorter; see ske_bridge.queue.cut_by_batch) and that is stopped by
the deadline did nothing wrong, so it is released (by an exception that is
both ``Reject``, which runs no failure handling, and ``Ignore``, which
``autoretry_for`` does not retry; see _batch_cut) and delivered again in a
later invocation; a message that has not started when too little is left is
released too. A task that had its full time and ran out of it fails with
``TimeLimitExceeded``. Interrupting a task needs
``SIGALRM`` and the main thread, which a gunicorn sync worker serves
requests on; elsewhere the platform's own timeout is the only limit.

Releases: one for the batch's time (not started, or cut) uses none of the
message's tries: its outcome says ``keeps_tries``, and the platform delivers
it again with its count kept. Every other release counts as a try: an error
setting up the task, ``Reject(requeue=True)``, a retry that could not be
published (delay 0), and a message held for its eta (the eta's delay).
"""

from __future__ import annotations

import functools
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
from ske_bridge.outcome import JobOutcome
from ske_bridge.queue import MAX_RELEASE_DELAY, batch_deadline, cut_by_batch

logger = logging.getLogger("ske_bridge")

#: The fewest seconds a task is started with. A message reached with less of
#: the batch's time left is released for another invocation.
MIN_START = 0.5


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
        # The task running in each thread: an ASGI app runs batches in worker
        # threads, several at once, through this one runner.
        self._local = threading.local()

    @property
    def _run(self) -> _Run | None:
        return getattr(self._local, "run", None)

    @_run.setter
    def _run(self, run: _Run | None) -> None:
        self._local.run = run

    def process(self, records: list[Any], options: dict[str, Any] | None = None, received: float | None = None) -> list[JobOutcome]:
        """One outcome per record, in the order they arrived; ``received``
        as ske_bridge.queue.batch_deadline takes it."""
        received = time.monotonic() if received is None else received
        deadline = batch_deadline(options or {}, received)
        return [self.run(r if isinstance(r, dict) else {}, deadline, received) for r in records]

    def run(self, record: dict[str, Any], deadline: float | None, received: float | None = None) -> JobOutcome:
        """One record's outcome, within the batch's ``deadline`` (see
        ske_bridge.queue.batch_deadline), which a batch that also holds jobs
        shares with them; ``received`` is when the batch arrived, by default
        now."""
        # Celery's current app is per thread: in a thread other than the one
        # that created the app, shared_task proxies and current_app would be
        # Celery's default app, which publishes nowhere the platform reads.
        self.app.set_current()
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
        cut, batch_limit = False, False
        if deadline is not None:
            started = time.monotonic()
            left = deadline - started
            if left < MIN_START:
                return JobOutcome.release_for_time(message_id, error="not started: the batch's time ran out")
            if not hard or left < hard:
                whole = deadline - (started if received is None else received)
                hard, cut, batch_limit = left, cut_by_batch(left, hard or None, whole), True
        if soft is not None and hard is not None and soft >= hard:
            soft = None

        run = self._run = _Run(soft, hard, cut=cut, batch_limit=batch_limit)
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
            return JobOutcome.release(message_id, delay=0, attempts=attempts, error=_describe(e))
        finally:
            self._run = None

        return self._outcome(message_id, attempts, info, run)

    def _outcome(self, message_id: str, attempts: int, info: Any, run: _Run) -> JobOutcome:
        from celery import states

        if info is None or info.state == states.IGNORED:
            return JobOutcome.success(message_id, attempts=attempts)

        exc = info.retval
        if info.state == states.RETRY:
            # The retry published its own message; this one is done.
            return JobOutcome.success(message_id, attempts=attempts, error=f"retried: {_describe(exc)}")
        if info.state == states.REJECTED:
            if exc is not None and exc is run.cut_reject:
                # Stopped at the batch's deadline: no try used.
                return JobOutcome.release_for_time(message_id, attempts=attempts, error="stopped at the batch's deadline")
            if getattr(exc, "requeue", False):
                return JobOutcome.release(message_id, delay=0, attempts=attempts, error=_describe(exc))
            if _raised_by_retry(exc):
                return JobOutcome.release(message_id, delay=0, attempts=attempts, error=f"the retry could not be sent: {_describe(exc.reason)}")

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

    def __init__(self, soft: float | None, hard: float | None, cut: bool = False, batch_limit: bool = False):
        self.soft = soft
        self.hard = hard
        #: Whether the hard limit is the batch's deadline, cutting the task
        #: short of its full time (ske_bridge.queue.cut_by_batch).
        self.cut = cut
        #: Whether the hard limit is the batch's deadline rather than the
        #: task's own limit.
        self.batch_limit = batch_limit
        #: The exception raised when the batch's deadline cut the task short.
        self.cut_reject: Any = None
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
        from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

        if not self._armed or frame is self._frame or frame.f_code is _Run.stop.__code__:
            return

        if self._phase_soft:
            self._phase_soft = False
            if self.hard:
                signal.setitimer(signal.ITIMER_REAL, max(self.hard - self.soft, 0.001))
            raise SoftTimeLimitExceeded(f"the task ran past its {self.soft:g}s soft time limit")
        if self.cut:
            self.cut_reject = _batch_cut()("stopped at the batch's deadline, to run again in a later delivery", requeue=True)
            raise self.cut_reject
        if self.batch_limit:
            raise TimeLimitExceeded(f"the task ran out of the batch's {_seconds(self.hard)}s")
        raise TimeLimitExceeded(f"the task ran past its {self.hard:g}s time limit")


@functools.cache
def _batch_cut() -> type:
    """The exception that stops a task at the batch's deadline.

    It is a ``Reject``, so the tracer records the task as rejected and runs
    no failure handling, and an ``Ignore`` as well, so ``autoretry_for``
    re-raises it rather than turn it into a retry: the tracer checks
    ``Reject`` first. A task's own bare ``except Exception`` still catches
    it, as it would any exception."""
    from celery.exceptions import Ignore, Reject

    class _BatchCut(Reject, Ignore):
        def __str__(self) -> str:
            return str(self.reason)

    return _BatchCut


def _seconds(value: float) -> str:
    """Seconds for a message: whole ones, or a tenth under one."""
    return f"{value:.0f}" if value >= 1 else f"{value:.1f}"


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
