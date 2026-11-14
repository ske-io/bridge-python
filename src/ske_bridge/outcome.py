"""What the platform should do with one delivered message now that its job
or task has run, or could not.

Deleting, releasing and failing all need the queue's receipt for the
delivery, which only the platform holds, so the bridge states the decision
and the platform carries it out:

- deleted: done; the message is acknowledged.
- released: deliver it again, after ``release_delay`` seconds. The release
  uses one of the job's tries, unless ``keeps_tries`` is set (with no delay):
  the job was not started, or was cut short, because its batch ran out of
  time, and the platform delivers the message again with its count of tries
  kept. ``keeps_tries`` is sent only when set.
- failed: it failed for good. Neither the bridge's jobs nor Celery keep a
  record that could run it again (a result backend stores a task's state and
  traceback, not a way to retry it), so every failure is also a dead letter:
  the platform keeps the message where it keeps failed messages, so it can
  still be inspected and redriven.

Jobs (ske_bridge.jobs) and Celery tasks answer in this one format.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class JobOutcome:
    message_id: str
    deleted: bool = False
    released: bool = False
    release_delay: int | None = None
    failed: bool = False
    dead_letter: bool = False
    attempts: int = 0
    error: str | None = None
    keeps_tries: bool = False

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "message_id": self.message_id,
            "deleted": self.deleted,
            "released": self.released,
            "release_delay": self.release_delay,
            "failed": self.failed,
            "dead_letter": self.dead_letter,
            "attempts": self.attempts,
            "error": self.error,
        }
        if self.keeps_tries:
            out["keeps_tries"] = True
        return out

    @classmethod
    def success(cls, message_id: str, attempts: int = 0, error: str | None = None) -> JobOutcome:
        return cls(message_id=message_id, deleted=True, attempts=attempts, error=error)

    @classmethod
    def release(cls, message_id: str, delay: int, attempts: int = 0, error: str | None = None) -> JobOutcome:
        """A release that uses one of the job's tries, after ``delay``
        seconds."""
        return cls(message_id=message_id, released=True, release_delay=delay, attempts=attempts, error=error)

    @classmethod
    def release_for_time(cls, message_id: str, attempts: int = 0, error: str | None = None) -> JobOutcome:
        """A release that keeps the job's tries: its batch ran out of time
        before it started, or cut it short. It is delivered again at once."""
        return cls(message_id=message_id, released=True, attempts=attempts, error=error, keeps_tries=True)

    @classmethod
    def failure(cls, message_id: str, error: str, attempts: int = 0) -> JobOutcome:
        return cls(message_id=message_id, failed=True, dead_letter=True, attempts=attempts, error=error)
