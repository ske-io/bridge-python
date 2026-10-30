"""What the platform should do with one message now that its task has run.

Deleting, releasing and failing all need the queue's receipt for the
delivery, which only the platform holds, so the bridge states the decision
and the platform carries it out.

A task that failed for good is finished. Celery keeps no record of it that
could run it again (a result backend stores its state and traceback, not a
way to retry it), so every such failure is a dead letter: the platform keeps
the message where the platform keeps failed messages, so it can still be
inspected and redriven.
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
