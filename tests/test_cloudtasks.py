import base64
import io
import json
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError

import pytest
from celery import Celery

from ske_bridge.celery import configure
from ske_bridge.celery.runner import QueueRunner
from ske_bridge.celery.message import decode
from tests.conftest import record


@pytest.fixture
def gcp(platform, monkeypatch):
    monkeypatch.setenv("SKE_PROVIDER", "gcp")
    monkeypatch.setenv("SKE_GCP_PROJECT", "test-project")
    monkeypatch.setenv("SKE_REGION", "us-central1")
    monkeypatch.setenv("SKE_QUEUE_PREFIX", "ske-shop-prod--")
    monkeypatch.setenv("SKE_QUEUES", "default,emails")
    sent, tokens = [], []

    def request(req, timeout):
        if req.full_url.startswith("http://metadata.google.internal/"):
            assert req.get_header("Metadata-flavor") == "Google"
            tokens.append(req)
            return io.BytesIO(b'{"access_token":"test-token","expires_in":3600}')
        assert req.full_url.startswith("https://cloudtasks.googleapis.com/v2/projects/test-project/locations/us-central1/queues/ske-shop-prod--")
        assert req.get_header("Authorization") == "Bearer test-token"
        sent.append((req.full_url, json.loads(req.data)["task"]))
        return io.BytesIO(b'{"name":"projects/test-project/locations/us-central1/queues/default/tasks/1"}')

    monkeypatch.setattr("ske_bridge.celery.cloudtasks.urlopen", request)
    app = Celery("cloudtasks-test", set_as_current=False)
    configure(app)
    yield app, sent, tokens
    app.close()


def payload(task):
    return base64.b64decode(task["httpRequest"]["body"]).decode()


def test_real_celery_envelopes_publish_route_and_run_without_broker_polling(gcp):
    app, sent, tokens = gcp
    calls = []

    @app.task(name="gcp.add")
    def add(a, b):
        calls.append(a + b)

    add.apply_async((2, 3), queue="emails")
    add.apply_async((3, 4))

    assert sent[0][0].endswith("ske-shop-prod--emails/tasks")
    assert sent[1][0].endswith("ske-shop-prod--default/tasks")
    assert len(tokens) == 1
    outcomes = QueueRunner(app).process([record(payload(task), message_id=str(i)) for i, (_, task) in enumerate(sent)])
    assert calls == [5, 7]
    assert all(outcome.deleted for outcome in outcomes)


def test_celery_retries_publish_new_tasks_with_incremented_retry_count(gcp):
    app, sent, _ = gcp

    @app.task(bind=True, name="gcp.retry")
    def retry(self):
        if self.request.retries == 0:
            raise self.retry(countdown=5)

    retry.apply_async(queue="emails")
    outcome = QueueRunner(app).process([record(payload(sent[0][1]))])[0]

    assert outcome.deleted
    assert len(sent) == 2
    assert sent[1][0].endswith("ske-shop-prod--emails/tasks")
    assert decode(payload(sent[1][1])).retries == 1
    assert "scheduleTime" in sent[1][1]


def test_unknown_queue_and_excessive_countdown_fail_before_dispatch(gcp):
    app, sent, _ = gcp
    with pytest.raises(ValueError, match="not configured"):
        app.send_task("unknown", queue="unconfigured")
    with pytest.raises(ValueError, match="30 days"):
        app.send_task("later", eta=datetime.now(UTC) + timedelta(days=31))
    assert sent == []


def test_dispatch_failure_is_reported_to_the_caller(gcp, monkeypatch):
    app, _, _ = gcp

    def refused(req, timeout):
        raise HTTPError(req.full_url, 403, "Forbidden", {}, None)

    monkeypatch.setattr("ske_bridge.celery.cloudtasks.urlopen", refused)
    with pytest.raises(Exception, match="Forbidden"):
        app.send_task("not-sent", retry=False)
