"""A Kombu transport that publishes Celery messages to the environment's
Cloud Tasks queues, through ske_bridge.cloudtasks.

Nothing is consumed here; the platform delivers each task over HTTP. A
message's ``eta`` becomes the task's schedule time, at most 30 days ahead.
"""

from __future__ import annotations

import json
import time
from urllib.error import URLError

from kombu.transport import virtual

from ske_bridge.celery.message import _timestamp
from ske_bridge.cloudtasks import create_task

MAX_DELAY = 30 * 24 * 60 * 60


class Channel(virtual.Channel):
    supports_fanout = False

    def _new_queue(self, queue, **kwargs):
        options = self.connection.client.transport_options
        if queue not in options["queues"]:
            raise ValueError(f"Queue {queue!r} is not configured in this SKE environment")

    def _put(self, queue, message, **kwargs):
        self._new_queue(queue)
        options = self.connection.client.transport_options

        eta = _timestamp(message.get("headers", {}).get("eta"))
        if eta is not None and eta.timestamp() - time.time() > MAX_DELAY:
            raise ValueError("Cloud Tasks schedules tasks at most 30 days ahead")

        create_task(options["project"], options["region"], f"{options['prefix']}{queue}", json.dumps(message), eta)

    def _get(self, queue, timeout=None):
        raise NotImplementedError("The platform delivers tasks over HTTP; no Celery worker runs")


class Transport(virtual.Transport):
    Channel = Channel
    driver_type = "cloudtasks"
    driver_name = "ske-cloudtasks"
    # CloudTasksUnavailable (429, 5xx) is a URLError too.
    connection_errors = (*virtual.Transport.connection_errors, URLError)
