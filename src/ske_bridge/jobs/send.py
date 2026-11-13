"""Sends job messages to the environment's queues.

On AWS the queues are SQS queues. The platform sets ``SQS_PREFIX`` and
``SQS_SUFFIX``, and a queue's URL is ``{SQS_PREFIX}/{name}{SQS_SUFFIX}``. The
default queue is ``SQS_QUEUE``, or ``default``; ``SQS_QUEUES`` lists every
queue the environment has. The region is ``AWS_REGION``, else
``AWS_DEFAULT_REGION``, and credentials come from boto3's default chain,
which on the platform is the function's role. boto3 (``ske-bridge[jobs]``)
is imported on the first send, so a process that never dispatches never
loads it.

On GCP (``SKE_PROVIDER=gcp``) the queues are Cloud Tasks queues, as for
Celery (see ske_bridge.cloudtasks): ``SKE_QUEUES`` names them, the default
is ``default``, and each is ``{SKE_QUEUE_PREFIX}{name}`` in
``SKE_GCP_PROJECT`` at ``SKE_REGION``.

The queue names a job may go to are those Celery may send to (see
ske_bridge.celery.settings): on AWS the default queue and those in
``SQS_QUEUES``, on GCP only those in ``SKE_QUEUES``.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from typing import Any

from ske_bridge import environment

#: The longest a job's start can be put off, in seconds: SQS's longest
#: delivery delay, on every provider alike.
MAX_DELAY = 900

_lock = threading.Lock()
_sqs_clients: dict[str, Any] = {}


def send(body: str, queue: str | None, delay: int) -> None:
    """Puts one message on the queue named ``queue`` (the default one when
    None), to be delivered ``delay`` seconds from now."""
    if environment.get("SKE_PROVIDER") == "gcp":
        _create_task(gcp_queue_name(queue), body, delay)
    else:
        _send_sqs(queue_url(sqs_queue_name(queue)), body, delay)


def _known(variable: str) -> list[str]:
    return [name.strip() for name in (environment.get(variable) or "").split(",") if name.strip()]


def _checked(name: str, known: list[str]) -> str:
    if name not in known:
        listed = ", ".join(known) if known else "none"
        raise ValueError(f"the environment has no queue named {name!r}; its queues are {listed}")
    return name


def sqs_queue_name(requested: str | None) -> str:
    """The SQS queue a job goes to, by name: the default one (``SQS_QUEUE``,
    else ``default``) when none is given. It must be the default one or one
    ``SQS_QUEUES`` lists."""
    default = environment.get("SQS_QUEUE") or "default"
    known = _known("SQS_QUEUES")
    if default not in known:
        known.insert(0, default)
    return _checked(requested or default, known)


def queue_url(name: str) -> str:
    prefix = environment.get("SQS_PREFIX")
    if prefix is None:
        raise RuntimeError("SQS_PREFIX is not set, so there is no queue to send the job to")
    return f"{prefix.rstrip('/')}/{name}{environment.get('SQS_SUFFIX') or ''}"


def gcp_queue_name(requested: str | None) -> str:
    """The Cloud Tasks queue a job goes to, by name: ``default`` when none is
    given. It must be one ``SKE_QUEUES`` lists; with none listed, no name
    is."""
    return _checked(requested or "default", _known("SKE_QUEUES"))


def _send_sqs(url: str, body: str, delay: int) -> None:
    message: dict[str, Any] = {"QueueUrl": url, "MessageBody": body}
    if delay > 0:
        message["DelaySeconds"] = delay
    sqs_client().send_message(**message)


def sqs_client() -> Any:
    """One boto3 SQS client per region, made on the first send. boto3's
    clients are safe to share between threads; making one is not, hence the
    lock."""
    region = environment.get("AWS_REGION") or environment.get("AWS_DEFAULT_REGION") or ""
    with _lock:
        client = _sqs_clients.get(region)
        if client is None:
            try:
                import boto3
            except ImportError as e:
                raise RuntimeError('sending jobs on AWS needs boto3: install "ske-bridge[jobs]"') from e
            client = boto3.client("sqs", region_name=region or None)
            _sqs_clients[region] = client
        return client


def _create_task(queue: str, body: str, delay: int) -> None:
    from ske_bridge.cloudtasks import create_task

    schedule = datetime.now(UTC) + timedelta(seconds=delay) if delay > 0 else None
    prefix = environment.get("SKE_QUEUE_PREFIX") or ""
    create_task(environment.get("SKE_GCP_PROJECT"), environment.get("SKE_REGION"), f"{prefix}{queue}", body, schedule)
