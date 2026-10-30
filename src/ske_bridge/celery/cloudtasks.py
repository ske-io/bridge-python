"""A Kombu transport that publishes Celery messages to the environment's
Cloud Tasks queues.

The platform creates each queue with an HTTP target that delivers its tasks to
the application and signs every push, so a task carries only the message: its
URL is a placeholder the queue's target replaces. Nothing is consumed here;
the platform delivers each task over HTTP. The access token comes from Cloud
Run's metadata server, so the image holds no credentials and no Google SDK is
needed.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import UTC
from urllib.error import URLError
from urllib.request import Request, urlopen

from kombu.transport import virtual

from ske_bridge.celery.message import _timestamp

TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
API = "https://cloudtasks.googleapis.com/v2"
MAX_DELAY = 30 * 24 * 60 * 60


class Channel(virtual.Channel):
    supports_fanout = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._access_token = ""
        self._expires_at = 0.0

    def _new_queue(self, queue, **kwargs):
        options = self.connection.client.transport_options
        if queue not in options["queues"]:
            raise ValueError(f"Queue {queue!r} is not configured in this SKE environment")

    def _put(self, queue, message, **kwargs):
        self._new_queue(queue)
        options = self.connection.client.transport_options

        task = {
            "httpRequest": {
                "httpMethod": "POST",
                "url": "https://cloudtasks.invalid/",
                "headers": {"Content-Type": "application/json"},
                "body": base64.b64encode(json.dumps(message).encode()).decode(),
            },
            "dispatchDeadline": "1800s",
        }
        eta = _timestamp(message.get("headers", {}).get("eta"))
        if eta is not None:
            if eta.timestamp() - time.time() > MAX_DELAY:
                raise ValueError("Cloud Tasks schedules tasks at most 30 days ahead")
            task["scheduleTime"] = eta.astimezone(UTC).isoformat()

        path = f"projects/{options['project']}/locations/{options['region']}/queues/{options['prefix']}{queue}/tasks"
        request = Request(f"{API}/{path}", json.dumps({"task": task}).encode(), {
            "Authorization": f"Bearer {self._token()}", "Content-Type": "application/json",
        }, method="POST")
        with urlopen(request, timeout=10) as reply:
            reply.read()

    def _token(self):
        if time.time() >= self._expires_at:
            request = Request(TOKEN_URL, headers={"Metadata-Flavor": "Google"})
            with urlopen(request, timeout=5) as reply:
                token = json.load(reply)
            self._access_token = token["access_token"]
            self._expires_at = time.time() + max(0, int(token["expires_in"]) - 60)
        return self._access_token

    def _get(self, queue, timeout=None):
        raise NotImplementedError("The platform delivers tasks over HTTP; no Celery worker runs")


class Transport(virtual.Transport):
    Channel = Channel
    driver_type = "cloudtasks"
    driver_name = "ske-cloudtasks"
    connection_errors = (*virtual.Transport.connection_errors, URLError)
