"""Runs jobs dispatched off the platform, in this process.

One daemon thread runs them, one at a time, in the order they fall due (the
time of dispatch plus the delay; those due at the same time in the order
they were dispatched), as the platform runs the jobs of a batch one after
another. It is started with the first job. A job that fails is logged, and
the next one runs.

The thread does not keep the process alive, so a process that exits (a
script, a management command) drops the jobs not run yet; set
``SKE_QUEUE_SYNC=1`` there to run each job before dispatch() returns. The
scheduler command opens a session instead (open_session()): the jobs
dispatched from then on, and the jobs they dispatch in turn, are waited for
by settle() before it exits.
"""

from __future__ import annotations

import contextvars
import heapq
import itertools
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

logger = logging.getLogger("ske_bridge")


@dataclass
class Session:
    """The jobs dispatched while a session is open, and what became of
    them."""

    #: Jobs not finished yet: label by key, with when each is due to start
    #: on the ``time.monotonic()`` clock.
    pending: dict[int, tuple[str, float]] = field(default_factory=dict)
    failed: list[str] = field(default_factory=list)
    closed: bool = False


@dataclass(frozen=True)
class Settled:
    """What became of a session's jobs, each as ``name [id]``."""

    failed: list[str]
    #: Not run, or still running, when the wait ended.
    unfinished: list[str]

    @property
    def ok(self) -> bool:
        return not (self.failed or self.unfinished)


_UNSET = object()

#: The session of the local job running now (None for one dispatched with no
#: session open), so the jobs it dispatches belong to the same session.
_job_session: contextvars.ContextVar[object] = contextvars.ContextVar("ske_job_session", default=_UNSET)


class Scheduler:
    def __init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self._condition = threading.Condition()
        self._due: list[tuple[float, int, str, Callable[[], None]]] = []
        self._order = itertools.count()
        self._thread: threading.Thread | None = None
        self._pid = os.getpid()
        self._session: Session | None = None

    def add(self, delay: float, label: str, run: Callable[[], None]) -> None:
        """Runs ``run`` in ``delay`` seconds, in the context it is added
        from, or later when other jobs run before it."""
        if self._pid != os.getpid():
            # A forked child has no scheduler thread, and the parent's jobs
            # are the parent's to run.
            self._reset()
        context = contextvars.copy_context()
        inherited = _job_session.get()
        with self._condition:
            # A job dispatched by a local job belongs to that job's session,
            # even one closed since, whose results are ignored; any other to
            # the session open now.
            owner = self._session if inherited is _UNSET else inherited
            session = owner if isinstance(owner, Session) and not owner.closed else None
            key = next(self._order)
            runs_at = time.monotonic() + delay

            def job() -> None:
                _job_session.set(owner)
                failed = False
                try:
                    run()
                except BaseException:
                    failed = True
                    logger.exception("ske: job %s failed", label)
                finally:
                    with self._condition:
                        if session is not None and not session.closed:
                            if failed:
                                session.failed.append(label)
                            session.pending.pop(key, None)
                        self._condition.notify_all()

            if session is not None:
                session.pending[key] = (label, runs_at)
            heapq.heappush(self._due, (runs_at, key, label, lambda: context.run(job)))
            if self._thread is None:
                self._thread = threading.Thread(target=self._loop, name="ske-jobs", daemon=True)
                self._thread.start()
            self._condition.notify_all()

    def _loop(self) -> None:
        while True:
            with self._condition:
                while True:
                    wait = self._due[0][0] - time.monotonic() if self._due else None
                    if wait is not None and wait <= 0:
                        _, _, _, run = heapq.heappop(self._due)
                        break
                    self._condition.wait(wait)
            run()

    def open_session(self) -> None:
        """Opens a session, unless one is open already."""
        if self._pid != os.getpid():
            self._reset()
        with self._condition:
            if self._session is None:
                self._session = Session()

    def settle(self, timeout: float) -> Settled:
        """Waits, at most ``timeout`` seconds, for the open session's jobs to
        run, and closes it. A job due to start after the wait would end is
        not waited for: it is reported unfinished at once, as is one still
        running when the wait ends. Without an open session there is nothing
        to wait for."""
        with self._condition:
            session = self._session
            if session is None:
                return Settled([], [])
            deadline = time.monotonic() + timeout
            try:
                while True:
                    waiting = [runs_at for _, runs_at in session.pending.values() if runs_at <= deadline]
                    left = deadline - time.monotonic()
                    if not waiting or left <= 0:
                        break
                    self._condition.wait(left)
                # A job finishing after this is not heard of: the session is
                # closed under the same lock its jobs report to.
                return Settled(list(session.failed), [label for label, _ in session.pending.values()])
            finally:
                session.closed = True
                session.pending.clear()
                self._session = None


_scheduler = Scheduler()


def schedule(delay: float, label: str, run: Callable[[], None]) -> None:
    """Runs ``run`` in this process ``delay`` seconds from now; see the
    module."""
    _scheduler.add(delay, label, run)


def open_session() -> None:
    """Keeps track of the jobs dispatched from now on, and of the jobs they
    dispatch, for settle(). Only the scheduler command opens one; without it
    nothing is kept, so a long-running app's failed jobs are logged and
    forgotten. Opening one while one is open keeps that one."""
    _scheduler.open_session()


def settle(timeout: float) -> Settled:
    """Waits for the open session's jobs; see Scheduler.settle()."""
    return _scheduler.settle(timeout)
