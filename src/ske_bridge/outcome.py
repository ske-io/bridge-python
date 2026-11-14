"""What the platform should do with one delivered message now that its job
or task has run, or could not.

Deleting, releasing and failing all need the queue's receipt for the
delivery, which only the platform holds, so the bridge states the decision
and the platform carries it out:

- deleted: done; the message is acknowledged.
- released: deliver it again, after ``release_delay`` seconds.
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "deleted": self.deleted,
            "released": self.released,
            "release_delay": self.release_delay,
            "failed": self.failed,
            "dead_letter": self.dead_letter,
            "attempts": self.attempts,
            "error": self.error,
        }

    @classmethod
    def success(cls, message_id: str, attempts: int = 0, error: str | None = None) -> JobOutcome:
        return cls(message_id=message_id, deleted=True, attempts=attempts, error=error)

    @classmethod
    def release(cls, message_id: str, delay: int | None = None, attempts: int = 0, error: str | None = None) -> JobOutcome:
        return cls(message_id=message_id, released=True, release_delay=delay, attempts=attempts, error=error)

    @classmethod
    def failure(cls, message_id: str, error: str, attempts: int = 0) -> JobOutcome:
        return cls(message_id=message_id, failed=True, dead_letter=True, attempts=attempts, error=error)
