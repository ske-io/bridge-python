"""Background jobs: a name, a handler, and dispatch() to run it later. ::

    from ske_bridge.jobs import dispatch, job

    @job("send-welcome-mail")
    def send_welcome(payload, ctx):
        ...

    send_welcome.dispatch({"user_id": 7}, delay=30)
    dispatch("send-welcome-mail", {"user_id": 7}, queue="emails")
    await dispatch_async("send-welcome-mail", {"user_id": 7})   # from async code

On the platform dispatch() sends the job to the environment's queue (see
ske_bridge.jobs.send) and the platform delivers it back to the application,
whose ``/__ske/queue`` route runs the handler (see ske_bridge.jobs.runner).
Off the platform it runs the handler in this process once the delay has
passed, one job at a time in the order they fall due, as the platform runs a
batch (see ske_bridge.jobs.local); with ``SKE_QUEUE_SYNC=1`` it runs it
before dispatch() returns, for tests. Either way the handler runs inside the
framework integration's ``around_job`` (a Flask app context, Django's
database connection upkeep), as on the platform.

The payload travels as JSON, so the handler gets a JSON copy of it in every
case: tuples become lists and dates must be strings already, on the
platform and off it alike.

A job is known to a process only once the module defining it is imported, so
define jobs in a module both the web process and the queue process import.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from ske_bridge import environment
from ske_bridge.jobs.message import JobMessage, encode
from ske_bridge.jobs.send import MAX_DELAY

__all__ = ["MAX_DELAY", "Job", "JobContext", "JobTimeout", "dispatch", "dispatch_async", "job"]

Handler = Callable[[Any, "JobContext"], Any]

_jobs: dict[str, Job] = {}
_lock = threading.Lock()
_around_job: Callable[[], AbstractContextManager[Any]] = nullcontext


class JobTimeout(BaseException):
    """Raised in a handler that runs past its time limit, where the bridge
    can interrupt it (see ske_bridge.jobs.runner). Like
    ``asyncio.CancelledError`` it is not an ``Exception``, so an
    ``except Exception`` in the handler does not swallow it."""


class JobContext:
    """What a job's handler is given besides its payload: ``id``, the same on
    every delivery of one dispatch; ``name``, as defined with job();
    ``attempts``, which delivery this is, from 1; ``cancelled``, a
    ``threading.Event`` set when the job runs out of time, when a handler
    should stop; and ``time_left()``."""

    __slots__ = ("_deadline", "attempts", "cancelled", "id", "name")

    def __init__(self, id: str, name: str, attempts: int, *, deadline: float | None = None):
        self.id = id
        self.name = name
        self.attempts = attempts
        self.cancelled = threading.Event()
        #: When the job's time runs out, on the ``time.monotonic()`` clock.
        self._deadline = deadline

    def time_left(self) -> float | None:
        """Seconds left before the job runs out of time, or None when it has
        no limit (off the platform)."""
        return None if self._deadline is None else max(self._deadline - time.monotonic(), 0.0)

    def __repr__(self) -> str:
        return f"<JobContext {self.name!r} [{self.id}] attempt {self.attempts}>"


class Job:
    """A defined job: its name and handler. Calling it calls the handler."""

    def __init__(self, name: str, handler: Handler):
        self.name = name
        self.handler = handler

    def dispatch(self, payload: Any = None, *, delay: int = 0, queue: str | None = None) -> str:
        """dispatch() for this job."""
        return dispatch(self.name, payload, delay=delay, queue=queue)

    async def dispatch_async(self, payload: Any = None, *, delay: int = 0, queue: str | None = None) -> str:
        """dispatch_async() for this job."""
        return await dispatch_async(self.name, payload, delay=delay, queue=queue)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.handler(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<Job {self.name!r}>"


def job(name: str) -> Callable[[Handler], Job]:
    """Defines the job ``name`` with the decorated handler, sync or async,
    which takes ``(payload, ctx)``.

    Defining a name twice is an error: a delivered message names its job, and
    must find exactly one handler. The one exception is the same function
    defined again (the same ``__module__`` and ``__qualname__``), as a module
    imported a second time does: the newer definition replaces the older."""
    if not isinstance(name, str) or name == "":
        raise TypeError("a job needs a name")

    def define(handler: Handler) -> Job:
        if not callable(handler):
            raise TypeError(f"job {name!r} needs a handler function")
        with _lock:
            existing = _jobs.get(name)
            if existing is not None:
                if _origin(existing.handler) is None or _origin(existing.handler) != _origin(handler):
                    raise ValueError(f"a job named {name!r} is already defined")
                existing.handler = handler
                return existing
            defined = _jobs[name] = Job(name, handler)
        return defined

    return define


def _origin(handler: Any) -> tuple[str, str] | None:
    module, qualname = getattr(handler, "__module__", None), getattr(handler, "__qualname__", None)
    return (module, qualname) if module and qualname else None


def get(name: str) -> Job | None:
    """The job ``name``, if it is defined."""
    return _jobs.get(name)


def defined() -> bool:
    """Whether this process defines any job."""
    return bool(_jobs)


def set_around_job(around: Callable[[], AbstractContextManager[Any]] | None) -> None:
    """For framework integrations: the context manager every job off the
    platform runs inside (a Flask app context, Django's connection upkeep),
    as the integration's queue runner runs it on the platform. None resets
    it."""
    global _around_job
    _around_job = around or nullcontext


def dispatch(name: str, payload: Any = None, *, delay: int = 0, queue: str | None = None) -> str:
    """Runs the job ``name`` with ``payload`` later, and returns the job's id
    once it is queued.

    ``delay`` is whole seconds from 0 to 900 before it runs; ``queue`` the
    name of one of the environment's queues, the default one when None.
    Raises when the job is not defined, the payload is not JSON or the queue
    refuses the message. On the platform the send blocks until the queue
    has the message; from async code use dispatch_async().
    """
    defined_job = get(name)
    if defined_job is None:
        raise LookupError(f"no job named {name!r} is defined")

    if not isinstance(delay, int) or isinstance(delay, bool) or not 0 <= delay <= MAX_DELAY:
        raise ValueError(f"a job's delay is a whole number of seconds from 0 to {MAX_DELAY}, not {delay!r}")

    try:
        text = json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError) as e:
        raise TypeError(f"the payload of job {name!r} is not a JSON value: {e}") from None

    id = str(uuid.uuid4())

    if environment.running_on_platform():
        from ske_bridge.jobs.send import send

        send(encode(JobMessage(name, json.loads(text), id)), queue, delay)
        return id

    def run() -> None:
        # The job as defined when it runs, as a delivered job finds it.
        current = get(name) or defined_job
        with _around_job():
            call(current.handler, json.loads(text), JobContext(id, name, 1))

    if environment.get("SKE_QUEUE_SYNC") == "1":
        run()
        return id

    from ske_bridge.jobs.local import schedule

    schedule(delay, f"{name} [{id}]", run)
    return id


async def dispatch_async(name: str, payload: Any = None, *, delay: int = 0, queue: str | None = None) -> str:
    """dispatch() from async code: the send runs in a worker thread
    (``asyncio.to_thread``), so the event loop keeps serving meanwhile."""
    return await asyncio.to_thread(dispatch, name, payload, delay=delay, queue=queue)


def call(handler: Handler, payload: Any, ctx: JobContext) -> Any:
    """Calls a handler, sync or async, to its end, with no time limit: an
    async handler runs in an event loop of its own, in a thread of its own
    when this thread already runs one."""
    result = handler(payload, ctx)
    if not inspect.isawaitable(result):
        return result

    async def wait() -> Any:
        return await result

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(wait())

    outcome: dict[str, Any] = {}
    context = contextvars.copy_context()

    def in_thread() -> None:
        try:
            outcome["value"] = context.run(asyncio.run, wait())
        except BaseException as e:
            outcome["error"] = e

    thread = threading.Thread(target=in_thread, name=f"ske-job-{ctx.name}")
    thread.start()
    thread.join()
    if "error" in outcome:
        raise outcome["error"]
    return outcome.get("value")
