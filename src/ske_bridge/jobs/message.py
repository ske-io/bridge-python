"""The body of one queued job: UTF-8 JSON ::

    {"job":"send-welcome-mail","payload":{...},"id":"<uuid>"}

``job`` is the name the handler was defined with, ``payload`` the JSON value
given to dispatch(), and ``id`` a UUID made once by dispatch() and kept on
every delivery of the message.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)


class InvalidMessage(ValueError):
    """A delivered body that holds no job message."""


@dataclass(frozen=True)
class JobMessage:
    job: str
    payload: Any
    id: str


def encode(message: JobMessage) -> str:
    """The message's body, compact, with non-ASCII text as it is."""
    return json.dumps({"job": message.job, "payload": message.payload, "id": message.id}, separators=(",", ":"), ensure_ascii=False)


def decode(body: Any) -> JobMessage:
    """The message a delivered body holds; raises InvalidMessage when it holds
    none."""
    if not isinstance(body, str):
        raise InvalidMessage("the body is not text")
    try:
        parsed = json.loads(body)
    except ValueError:
        raise InvalidMessage("the body is not JSON") from None
    if not isinstance(parsed, dict):
        raise InvalidMessage("the body is not a JSON object")

    job, id = parsed.get("job"), parsed.get("id")
    if not isinstance(job, str) or job == "":
        raise InvalidMessage('"job" is not a job name')
    if "payload" not in parsed:
        raise InvalidMessage('"payload" is missing')
    if not isinstance(id, str) or not _UUID.fullmatch(id):
        raise InvalidMessage('"id" is not a UUID')
    return JobMessage(job, parsed["payload"], id)


def is_job_message(body: Any) -> bool:
    """Whether a delivered body is meant as a job message: a JSON object with
    a ``"job"`` key. A Celery envelope never is one. Such a body is a job
    message even when decode() refuses it, so it is never handed to Celery."""
    if not isinstance(body, str) or not body.lstrip().startswith("{"):
        return False
    try:
        parsed = json.loads(body)
    except ValueError:
        return False
    return isinstance(parsed, dict) and "job" in parsed
