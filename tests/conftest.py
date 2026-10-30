import base64
import json
from pathlib import Path

import pytest
from kombu.transport import SQS

FIXTURES = Path(__file__).parent / "fixtures"


class StubSQS:
    """Stands in for boto3's SQS client: kombu's transport encodes and sends
    the message as it would to AWS, and the stub keeps what it was sent."""

    def __init__(self, sent):
        self.sent = sent

    def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return {"MessageId": f"m-{len(self.sent)}"}

    def get_queue_attributes(self, **kwargs):
        return {"Attributes": {"ApproximateNumberOfMessages": "0"}}


@pytest.fixture(autouse=True)
def sqs_sent(monkeypatch):
    sent = []
    monkeypatch.setattr(SQS.Channel, "sqs", lambda self, queue=None: StubSQS(sent))
    return sent


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in (
        "SKE_PROJECT_TYPE", "SKE_ENVIRONMENT", "SKE_CELERY_APP", "CELERY_APP", "SKE_PROVIDER",
        "SQS_SUFFIX", "SQS_QUEUES", "SKE_SCHEDULED_AT", "MAIL_HOST", "MAIL_PORT",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def platform(monkeypatch):
    monkeypatch.setenv("SKE_ENVIRONMENT", "production")
    monkeypatch.setenv("SKE_CELERY_APP", "tests.celery_app:app")


@pytest.fixture
def celery_app():
    from tests.celery_app import app, calls

    calls.clear()
    return app


@pytest.fixture
def publish(celery_app, sqs_sent):
    """Publishes a task through kombu's SQS transport and returns the SQS
    message body it sent."""

    def publish(name, args=(), kwargs=None, **options):
        celery_app.tasks[name].apply_async(args, kwargs or {}, **options)
        return sqs_sent[-1]["MessageBody"]

    return publish


def record(body, message_id="msg-1", attempts=1, queue="default--shop-production"):
    """One record as the platform sends it to /__ske/queue."""
    return {"message_id": message_id, "body": body, "queue": queue, "attempts": attempts}


def envelope(body):
    return json.loads(base64.b64decode(body))


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


class SMTPCatcher:
    """A plain SMTP server like the platform's mail service: no STARTTLS, no
    AUTH offered, every message accepted and kept."""

    def __init__(self):
        import socket
        import threading

        self.messages = []
        self._server = socket.create_server(("127.0.0.1", 0))
        self.port = self._server.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while True:
            try:
                conn, _ = self._server.accept()
            except OSError:
                return
            with conn, conn.makefile("rwb") as stream:
                def reply(line):
                    stream.write(line + b"\r\n")
                    stream.flush()

                reply(b"220 localhost")
                while line := stream.readline():
                    command = line.strip().upper()
                    if command.startswith((b"EHLO", b"HELO")):
                        reply(b"250-localhost")
                        reply(b"250 OK")
                    elif command == b"DATA":
                        reply(b"354 go ahead")
                        data = b""
                        while (chunk := stream.readline()) not in (b".\r\n", b""):
                            data += chunk
                        self.messages.append(data)
                        reply(b"250 OK")
                    elif command == b"QUIT":
                        reply(b"221 Bye")
                        break
                    else:
                        reply(b"250 OK")

    def close(self):
        self._server.close()


@pytest.fixture
def smtp_catcher():
    catcher = SMTPCatcher()
    yield catcher
    catcher.close()
