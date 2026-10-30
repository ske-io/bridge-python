import base64
import json

import pytest

from ske_bridge.celery.message import InvalidMessage, decode
from tests.conftest import envelope, fixture


def test_decodes_the_message_kombus_sqs_transport_sends():
    body = fixture("sqs-celery-event.json")["Records"][0]["body"]

    message = decode(body)

    assert message.task == "tests.add"
    assert message.id == "5f0c8a9e-3c1d-4a8b-9a51-2b7d1e6c4f10"
    assert message.args == [2, 3]
    assert message.kwargs == {}
    assert message.embed == {"callbacks": None, "errbacks": None, "chain": None, "chord": None}
    assert message.retries == 0
    assert message.eta is None and message.expires is None
    assert message.properties["delivery_info"] == {"exchange": "", "routing_key": "default"}


def test_decodes_what_the_installed_kombu_publishes(publish):
    body = publish("tests.add", (4, 5), countdown=60, expires=600, time_limit=30, soft_time_limit=20)

    message = decode(body)

    assert message.args == [4, 5]
    assert message.eta is not None and message.eta.tzinfo is not None
    assert message.expires is not None
    assert message.time_limits == (30.0, 20.0)


def test_accepts_the_envelope_as_plain_json(publish):
    raw = json.dumps(envelope(publish("tests.add", (1, 1))))

    assert decode(raw).args == [1, 1]


@pytest.mark.parametrize("body", ["", "not base64 !", base64.b64encode(b"[1, 2]").decode(), '{"task": "x"}'])
def test_refuses_what_is_not_a_task_message(body):
    with pytest.raises(InvalidMessage):
        decode(body)


def test_refuses_protocol_1_messages():
    v1 = {"body": base64.b64encode(json.dumps({"task": "tests.add", "id": "1", "args": [], "kwargs": {}}).encode()).decode(),
          "content-type": "application/json", "content-encoding": "utf-8", "headers": {},
          "properties": {"body_encoding": "base64"}}

    with pytest.raises(InvalidMessage, match="protocol 2"):
        decode(json.dumps(v1))


def test_refuses_content_the_app_does_not_accept(publish, celery_app):
    from kombu.serialization import prepare_accept_content

    env = envelope(publish("tests.add", (1, 1)))
    env["content-type"] = "application/x-python-serialize"

    with pytest.raises(InvalidMessage, match="cannot be decoded"):
        decode(json.dumps(env), prepare_accept_content(celery_app.conf.accept_content))
