"""Creates tasks in the environment's Cloud Tasks queues, for jobs
(ske_bridge.jobs.send) and Celery (ske_bridge.celery.cloudtasks) alike.

The platform creates each queue with an HTTP target that delivers its tasks to
the application and signs every push, so a task carries only the message: its
URL is a placeholder the queue's target replaces. The access token comes from
Cloud Run's metadata server, so the image holds no credentials and no Google
SDK is needed.
"""

from __future__ import annotations

import base64
import json
import threading
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
API = "https://cloudtasks.googleapis.com/v2"

#: Seconds before the token's expiry that a new one is fetched.
TOKEN_EARLY = 60

_lock = threading.Lock()
_token: dict[str, Any] = {"value": "", "expires_at": 0.0}


class CloudTasksError(Exception):
    """Cloud Tasks or the metadata server refused a request; the message
    carries the HTTP status and the reply's body."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class CloudTasksUnavailable(CloudTasksError, URLError):
    """A refusal worth trying again: 429 or a 5xx. It is a URLError, as an
    unreachable server is, so Kombu retries it as a connection error."""

    def __init__(self, message: str, status: int | None = None):
        CloudTasksError.__init__(self, message, status)
        self.reason = message

    def __str__(self) -> str:
        return CloudTasksError.__str__(self)


def create_task(project: str | None, region: str | None, queue: str, body: str, schedule: datetime | None = None) -> None:
    """Creates a task carrying ``body`` in the queue ``queue`` (its full
    name, prefix included), delivered at ``schedule`` or at once."""
    task: dict[str, Any] = {
        "httpRequest": {
            "httpMethod": "POST",
            # A placeholder: the queue's HTTP target sets where it goes.
            "url": "https://cloudtasks.invalid/",
            "headers": {"Content-Type": "application/json"},
            "body": base64.b64encode(body.encode()).decode(),
        },
        "dispatchDeadline": "1800s",
    }
    if schedule is not None:
        task["scheduleTime"] = schedule.astimezone(UTC).isoformat()

    path = f"projects/{project}/locations/{region}/queues/{queue}/tasks"
    request = Request(f"{API}/{path}", json.dumps({"task": task}).encode(), {
        "Authorization": f"Bearer {access_token()}", "Content-Type": "application/json",
    }, method="POST")
    _send(request, timeout=10, what=f"Cloud Tasks refused the task for queue {queue}")


def access_token() -> str:
    """The service account's access token, fetched again a minute before it
    expires."""
    with _lock:
        if time.time() >= _token["expires_at"]:
            request = Request(TOKEN_URL, headers={"Metadata-Flavor": "Google"})
            token = json.loads(_send(request, timeout=5, what="the metadata server refused the access token"))
            _token["value"] = token["access_token"]
            _token["expires_at"] = time.time() + max(0, int(token["expires_in"]) - TOKEN_EARLY)
        return _token["value"]


def _send(request: Request, timeout: float, what: str) -> bytes:
    try:
        with urlopen(request, timeout=timeout) as reply:
            return reply.read()
    except HTTPError as e:
        try:
            detail = e.read().decode(errors="replace").strip()
        except Exception:
            detail = ""
        message = f"{what}: HTTP {e.code} {e.reason}" + (f": {detail}" if detail else "")
        kind = CloudTasksUnavailable if e.code == 429 or e.code >= 500 else CloudTasksError
        raise kind(message, e.code) from None
