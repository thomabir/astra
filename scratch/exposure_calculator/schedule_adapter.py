"""Schedule adapter: the only place that knows about astra's Schedule/Action.

Responsibilities:
  - Find object actions whose exptime is the sentinel "auto".
  - Rewrite those actions with a concrete exptime (returns a new Schedule; original untouched).
  - Archive the original schedule to a timestamped file before rewriting.

This is purely data transformation — no camera calls, no analysis.
TDD: build Schedule objects in-memory, assert on the returned Schedule.
"""

from datetime import datetime, UTC
from pathlib import Path

from astra.scheduler import Schedule


SENTINEL = "auto"


def has_auto_exptime(action) -> bool:
    """True if this action is an object action requesting automatic exposure."""
    return (
        action.action_type == "object"
        and action.action_value.get("exptime") == SENTINEL
    )


def rewrite_exptime(schedule: Schedule, action_index: int, exptime: float) -> Schedule:
    """Return a new Schedule with the given action's exptime replaced.

    The original Schedule object is not mutated.
    """
    raise NotImplementedError


def archive_schedule(schedule: Schedule, archive_dir: Path) -> Path:
    """Write the schedule to a timestamped JSONL file and return the path.

    Called before any rewrite so the original intent is always recoverable.
    """
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = archive_dir / f"schedule_archive_{timestamp}.jsonl"
    schedule.save_to_jsonl(path)
    return path
