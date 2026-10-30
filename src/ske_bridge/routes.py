"""The platform's internal routes, independent of any framework.

The Django middleware and the WSGI wrapper the Flask extension installs both
hand ``/__ske/*`` requests here, before the framework's own routing,
middleware and hooks see them: a login requirement, CSRF protection or an
``ALLOWED_HOSTS`` check has nothing to say about a request the platform
built for itself.

Two routes answer:

- ``POST /__ske/queue`` runs the delivered queue messages and reports one
  outcome per message.
- ``GET /__ske/health`` is the readiness route the platform probes before it
  sends traffic. It returns a static status, so it is the one route outside
  traffic may reach, for load balancer checks.

The platform runs commands and the scheduler as processes in the container,
so neither has a route here.
"""

from __future__ import annotations

import hmac
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger("ske_bridge")

INTERNAL_HEADER = "X-Ske-Internal"
PREFIX = "/__ske/"
QUEUE_PATH = "/__ske/queue"
HEALTH_PATH = "/__ske/health"


class Runner(Protocol):
    def process(self, records: list[Any], options: dict[str, Any] | None = None) -> list[Any]: ...


@dataclass(frozen=True)
class Reply:
    status: int
    body: dict[str, Any] = field(default_factory=dict)

    def content(self) -> bytes:
        return json.dumps(self.body).encode()


NOT_FOUND = Reply(404, {"error": "not found"})


def is_internal(value: str | None) -> bool:
    """Whether a request carries the header only the platform's own requests
    do.

    The container's entry process strips this header from every request that
    arrives from outside and refuses any outside request for ``/__ske/*``, so
    this is the second of two locks rather than the only one. It exists
    because the first lives in a different process: if the application is
    ever fronted by something else, this still holds.
    """
    return isinstance(value, str) and hmac.compare_digest(value.encode(), b"true")


class Routes:
    """Answers the internal routes. The queue runner is built on the first
    delivery, so a process that only serves web traffic never imports Celery."""

    def __init__(self, runner: Callable[[], Runner]):
        self._make_runner = runner
        self._runner: Runner | None = None

    @staticmethod
    def handles(path: str) -> bool:
        return path.startswith(PREFIX)

    def dispatch(self, method: str, path: str, internal: str | None, body: bytes) -> Reply:
        if path == HEALTH_PATH:
            if method not in ("GET", "HEAD"):
                return Reply(405, {"error": "method not allowed"})
            return Reply(200, {"status": "ok"})

        # A missing or wrong header is answered as though the route did not
        # exist, which is what the platform does too: telling a caller these
        # paths are here and merely forbidden is more than they need to know.
        if not is_internal(internal):
            return NOT_FOUND

        if path == QUEUE_PATH:
            if method != "POST":
                return Reply(405, {"error": "method not allowed"})
            return self._queue(body)

        return NOT_FOUND

    def _queue(self, body: bytes) -> Reply:
        """Runs a batch: ``{"source", "records": [...], "options": {...}}`` in,
        ``{"processed", "outcomes": [...]}`` out.

        Anything other than a 200 with outcomes tells the platform nothing
        ran, and it delivers every message of the batch again. That is the answer
        for a batch that cannot be read and for an application that cannot run
        jobs at all (Celery missing, its app not found): retrying is right
        once the deployment is fixed, and the platform's dead-letter queue
        catches what never runs.
        """
        try:
            payload = json.loads(body or b"{}")
        except ValueError:
            return Reply(400, {"error": "the queue payload is not JSON"})
        if not isinstance(payload, dict):
            return Reply(400, {"error": "the queue payload is not an object"})

        records = payload.get("records")
        records = records if isinstance(records, list) else []
        options = payload.get("options")
        options = options if isinstance(options, dict) else {}

        try:
            if self._runner is None:
                self._runner = self._make_runner()
            outcomes = self._runner.process(records, options)
        except Exception as e:
            logger.exception("ske: the queue cannot run jobs")
            return Reply(500, {"error": f"{type(e).__name__}: {e}"})

        return Reply(200, {
            "processed": len(outcomes),
            "outcomes": [o.to_dict() for o in outcomes],
        })
