"""Runs jobs dispatched off the platform, in this process.

One daemon thread runs them, one at a time, in the order they fall due (the
time of dispatch plus the delay; those due at the same time in the order
they were dispatched), as the platform runs the jobs of a batch one after
another. It is started with the first job. A job that fails is logged, and
the next one runs.

The thread does not keep the process alive, so a process that exits (a
script, a management command) drops the jobs not run yet; set
``SKE_QUEUE_SYNC=1`` there to run each job before dispatch() returns.
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

logger = logging.getLogger("ske_bridge")


class Scheduler:
    def __init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self._condition = threading.Condition()
        self._due: list[tuple[float, int, str, Callable[[], None]]] = []
        self._order = itertools.count()
        self._thread: threading.Thread | None = None
        self._pid = os.getpid()

    def add(self, delay: float, label: str, run: Callable[[], None]) -> None:
        """Runs ``run`` in ``delay`` seconds, in the context it is added
        from, or later when other jobs run before it."""
        if self._pid != os.getpid():
            # A forked child has no scheduler thread, and the parent's jobs
            # are the parent's to run.
            self._reset()
        context = contextvars.copy_context()
        with self._condition:
            heapq.heappush(self._due, (time.monotonic() + delay, next(self._order), label, lambda: context.run(run)))
            if self._thread is None:
                self._thread = threading.Thread(target=self._loop, name="ske-jobs", daemon=True)
                self._thread.start()
            self._condition.notify()

    def _loop(self) -> None:
        while True:
            with self._condition:
                while True:
                    wait = self._due[0][0] - time.monotonic() if self._due else None
                    if wait is not None and wait <= 0:
                        _, _, label, run = heapq.heappop(self._due)
                        break
                    self._condition.wait(wait)
            try:
                run()
            except BaseException:
                logger.exception("ske: job %s failed", label)


_scheduler = Scheduler()


def schedule(delay: float, label: str, run: Callable[[], None]) -> None:
    """Runs ``run`` in this process ``delay`` seconds from now; see the
    module."""
    _scheduler.add(delay, label, run)
