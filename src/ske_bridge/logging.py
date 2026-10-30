"""Logs in the structured format the platform's logs page reads.

On the platform every log record becomes one JSON line on stderr::

    {"message": "...", "context": {...}, "level_name": "ERROR",
     "channel": "django.request", "datetime": "2026-10-28T09:30:00.123456+00:00",
     "extra": {}}

``context`` carries the fields passed with ``extra=``, the request's id from
the platform's ``X-Request-Id`` header as ``request_id``, and, for a record
with an exception, ``exception``: ``class``, ``message``, ``file`` (where it
was raised, as ``path:line``), ``trace`` (the calls that led there, most
recent first, each ``path:line``) and ``previous``, the exception it was
raised from. The logs page shows the level, message and context, and the
exception with its trace.

What is set up, once, when the app starts:

- The root logger writes to stderr in this format; a handler it already has
  for stdout or stderr is given the format instead.
- Every other logger's stdout or stderr handlers are given the format too:
  Django's, Flask's, gunicorn's. A logger whose records reach the root
  logger anyway loses those handlers, so no record is written twice.
- Levels are left as the app set them.

Set ``SKE_LOG_FORMAT=plain`` to keep the app's own logging as it is.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import traceback
from datetime import UTC, datetime
from typing import Any

from ske_bridge import environment

#: The platform's id for the request being served, from ``X-Request-Id``.
request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("ske_request_id", default=None)

#: Attributes every LogRecord has; anything else on a record came from
#: ``extra=`` and belongs in its context.
_RECORD_ATTRIBUTES = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context = {key: value for key, value in vars(record).items() if key not in _RECORD_ATTRIBUTES}
        current = request_id.get()
        if current is not None:
            context.setdefault("request_id", current)
        if record.exc_info and record.exc_info[1] is not None:
            context["exception"] = exception(record.exc_info[1])

        return json.dumps({
            "message": record.getMessage(),
            "context": context,
            "level_name": record.levelname,
            "channel": record.name,
            "datetime": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "extra": {},
        }, default=str)


def exception(error: BaseException, depth: int = 0) -> dict[str, Any]:
    """An exception as the logs page reads it."""
    frames = traceback.extract_tb(error.__traceback__)
    kind = type(error)
    name = kind.__qualname__ if kind.__module__ == "builtins" else f"{kind.__module__}.{kind.__qualname__}"

    data: dict[str, Any] = {
        "class": name,
        "message": str(error),
        "file": f"{frames[-1].filename}:{frames[-1].lineno}" if frames else None,
        "trace": [f"{frame.filename}:{frame.lineno}" for frame in reversed(frames[:-1])],
    }

    cause = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
    if cause is not None and depth < 8:
        data["previous"] = exception(cause, depth + 1)
    return data


def enabled() -> bool:
    """On the platform, unless the app opted out with
    ``SKE_LOG_FORMAT=plain``."""
    return environment.running_on_platform() and (environment.get("SKE_LOG_FORMAT") or "json").lower() != "plain"


def configure(manager: logging.Manager | None = None) -> None:
    """Sets up the structured format when enabled(). Safe to call more than
    once. ``manager`` is the logger tree, the process's own by default."""
    if not enabled():
        return

    manager = manager or logging.Logger.manager
    formatter = JsonFormatter()
    root = manager.root

    terminal = [h for h in root.handlers if _writes_to_terminal(h)]
    if terminal:
        for handler in terminal:
            handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(formatter)
        root.addHandler(handler)

    for logger in list(manager.loggerDict.values()):
        if not isinstance(logger, logging.Logger):
            continue
        for handler in [h for h in logger.handlers if _writes_to_terminal(h)]:
            if _reaches_root(logger):
                logger.removeHandler(handler)
            else:
                handler.setFormatter(formatter)


def _writes_to_terminal(handler: logging.Handler) -> bool:
    return (
        type(handler) is logging.StreamHandler
        and getattr(handler, "stream", None) in (sys.stdout, sys.stderr, sys.__stdout__, sys.__stderr__)
    )


def _reaches_root(logger: logging.Logger) -> bool:
    current: logging.Logger | None = logger
    while current is not None and current.parent is not None:
        if not current.propagate:
            return False
        current = current.parent
    return True
