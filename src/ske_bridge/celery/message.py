"""Reads a Celery task message as the queue delivered it.

Celery hands kombu a message envelope, and kombu's transport puts it on the
broker. On SQS the message body is the envelope as JSON, base64-encoded by
default (the transport's ``sqs_base64_encoding`` option); on Pub/Sub it is the
JSON itself. Either is accepted::

    {
      "body": "<base64 of [args, kwargs, embed]>",
      "content-type": "application/json",
      "content-encoding": "utf-8",
      "headers": {"task": "...", "id": "...", "retries": 0, "eta": null, ...},
      "properties": {"body_encoding": "base64", "delivery_info": {...}, ...}
    }

This is task protocol 2, Celery's default since 4.0: the task's name, id and
options are headers, and the body is ``[args, kwargs, embed]``, where embed
carries the callbacks, errbacks, chain and chord. Protocol 1 messages are
refused.

The body is decoded with kombu's serializers and the app's
``accept_content``, as a worker decodes it, so a content type the application
does not accept (pickle, unless it opted in) is refused here too.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


class InvalidMessage(ValueError):
    """A delivered message that is not a Celery task this bridge can run."""


@dataclass(frozen=True)
class TaskMessage:
    task: str
    id: str
    args: list[Any]
    kwargs: dict[str, Any]
    embed: dict[str, Any]
    headers: dict[str, Any]
    properties: dict[str, Any] = field(default_factory=dict)

    @property
    def retries(self) -> int:
        try:
            return max(int(self.headers.get("retries") or 0), 0)
        except (TypeError, ValueError):
            return 0

    @property
    def eta(self) -> datetime | None:
        return _timestamp(self.headers.get("eta"))

    @property
    def expires(self) -> datetime | None:
        return _timestamp(self.headers.get("expires"))

    @property
    def time_limits(self) -> tuple[float | None, float | None]:
        """``(hard, soft)``, in seconds, as the message's ``timelimit`` header
        orders them."""
        limits = self.headers.get("timelimit") or (None, None)
        try:
            hard, soft = limits
        except (TypeError, ValueError):
            return None, None
        return _seconds(hard), _seconds(soft)


def decode(raw: str, accept: Any = None) -> TaskMessage:
    """Decodes one delivered message body. ``accept`` is the set of content
    types the app accepts, as ``kombu.serialization.prepare_accept_content``
    returns it; None accepts kombu's defaults."""
    from kombu.serialization import loads

    envelope = _envelope(raw)

    headers = envelope.get("headers")
    if not isinstance(headers, dict) or "task" not in headers:
        raise InvalidMessage("not a task protocol 2 message (Celery's default since 4.0)")

    properties = envelope.get("properties")
    properties = properties if isinstance(properties, dict) else {}

    body = envelope.get("body")
    if properties.get("body_encoding") == "base64":
        try:
            body = base64.b64decode(body or "", validate=True)
        except (binascii.Error, TypeError) as e:
            raise InvalidMessage(f"the task body is not valid base64: {e}") from None

    try:
        decoded = loads(
            body,
            envelope.get("content-type"),
            envelope.get("content-encoding") or "utf-8",
            accept=accept,
        )
        args, kwargs, embed = decoded
    except (TypeError, ValueError) as e:
        raise InvalidMessage(f"the task body is not [args, kwargs, embed]: {e}") from None
    except Exception as e:
        # kombu refuses a content type outside accept_content with its own
        # error types; any of them means the body cannot be run.
        raise InvalidMessage(f"the task body cannot be decoded: {e}") from None

    if not isinstance(args, (list, tuple)) or not isinstance(kwargs, dict):
        raise InvalidMessage("the task body is not [args, kwargs, embed]")

    return TaskMessage(
        task=str(headers["task"]),
        id=str(headers.get("id") or ""),
        args=list(args),
        kwargs=kwargs,
        embed=embed if isinstance(embed, dict) else {},
        headers=headers,
        properties=properties,
    )


def _envelope(raw: str) -> dict[str, Any]:
    """The envelope from a body that is either its JSON or that JSON
    base64-encoded."""
    text = raw.strip() if isinstance(raw, str) else ""
    if not text:
        raise InvalidMessage("the message is empty")

    if not text.startswith("{"):
        try:
            text = base64.b64decode(text, validate=True).decode()
        except (binascii.Error, UnicodeDecodeError):
            raise InvalidMessage("the message is neither JSON nor base64-encoded JSON") from None

    try:
        envelope = json.loads(text)
    except ValueError:
        raise InvalidMessage("the message is not JSON") from None
    if not isinstance(envelope, dict):
        raise InvalidMessage("the message is not a Celery message envelope")
    return envelope


def _timestamp(value: Any) -> datetime | None:
    """An ISO 8601 header as an aware datetime. Celery writes ``eta`` and
    ``expires`` in UTC; a naive value is read as UTC."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _seconds(value: Any) -> float | None:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None
