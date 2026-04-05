"""Tests for schedule_adapter — pure data manipulation, no hardware.

Builds Schedule objects in-memory; asserts on returned Schedules.
No camera, no FITS files, no observatory instance needed.
"""

from dataclasses import dataclass, field
from datetime import datetime, UTC, timedelta
from typing import Any

import pytest

from astra.action_configs import BaseActionConfig
from astra.scheduler import Action, Schedule

from exposure_calculator.schedule_adapter import (
    SENTINEL,
    archive_schedule,
    has_auto_exptime,
    rewrite_exptime,
)


# ---------------------------------------------------------------------------
# Minimal stub: accepts any exptime value, including the "auto" sentinel.
# Extends BaseActionConfig so that to_jsonable() and get() work correctly.
# ---------------------------------------------------------------------------

@dataclass
class _FakeActionValue(BaseActionConfig):
    exptime: Any = SENTINEL

    def validate(self):
        pass  # no type enforcement needed in tests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_action(
    action_type: str = "object",
    exptime: Any = SENTINEL,
    device_name: str = "cam",
    start_hour: int = 21,
) -> Action:
    t0 = datetime(2026, 4, 5, start_hour, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)
    return Action(
        device_name=device_name,
        action_type=action_type,
        action_value=_FakeActionValue(exptime=exptime),
        start_time=t0,
        end_time=t1,
    )


def make_schedule(*actions: Action) -> Schedule:
    # Schedule sorts by start_time, so each action must have a distinct time
    # to preserve insertion order.
    staggered = []
    for i, a in enumerate(actions):
        t0 = datetime(2026, 4, 5, 20 + i, 0, tzinfo=UTC)
        staggered.append(Action(
            device_name=a.device_name,
            action_type=a.action_type,
            action_value=a.action_value,
            start_time=t0,
            end_time=t0 + timedelta(hours=1),
        ))
    return Schedule(staggered)


# ---------------------------------------------------------------------------
# has_auto_exptime
# ---------------------------------------------------------------------------

def test_auto_sentinel_is_detected():
    action = make_action("object", exptime=SENTINEL)
    assert has_auto_exptime(action)


def test_concrete_exptime_is_not_sentinel():
    action = make_action("object", exptime=120.0)
    assert not has_auto_exptime(action)


def test_non_object_action_is_not_sentinel():
    action = make_action("autofocus", exptime=5.0)
    assert not has_auto_exptime(action)


# ---------------------------------------------------------------------------
# rewrite_exptime
# ---------------------------------------------------------------------------

def test_rewrite_sets_exptime_on_target_action():
    schedule = make_schedule(
        make_action("autofocus", exptime=5.0),
        make_action("object", exptime=SENTINEL),
    )
    new_schedule = rewrite_exptime(schedule, action_index=1, exptime=90.0)
    assert new_schedule[1].action_value.get("exptime") == 90.0


def test_rewrite_does_not_mutate_original_schedule():
    schedule = make_schedule(make_action("object", exptime=SENTINEL))
    _ = rewrite_exptime(schedule, action_index=0, exptime=90.0)
    assert schedule[0].action_value.get("exptime") == SENTINEL


def test_rewrite_leaves_other_actions_unchanged():
    schedule = make_schedule(
        make_action("object", exptime=SENTINEL),
        make_action("object", exptime=SENTINEL),
    )
    new_schedule = rewrite_exptime(schedule, action_index=0, exptime=90.0)
    assert new_schedule[1].action_value.get("exptime") == SENTINEL


# ---------------------------------------------------------------------------
# archive_schedule
# ---------------------------------------------------------------------------

def test_archive_creates_a_file(tmp_path):
    schedule = make_schedule(make_action("object", exptime=SENTINEL))
    path = archive_schedule(schedule, archive_dir=tmp_path)
    assert path.exists()


def test_archive_filename_contains_timestamp(tmp_path):
    schedule = make_schedule(make_action("object", exptime=SENTINEL))
    path = archive_schedule(schedule, archive_dir=tmp_path)
    assert "schedule_archive_" in path.name


def test_archive_preserves_original_sentinel(tmp_path):
    schedule = make_schedule(make_action("object", exptime=SENTINEL))
    path = archive_schedule(schedule, archive_dir=tmp_path)
    assert SENTINEL in path.read_text()


def test_two_archives_have_distinct_names(tmp_path):
    import time
    schedule = make_schedule(make_action("object", exptime=SENTINEL))
    p1 = archive_schedule(schedule, archive_dir=tmp_path)
    time.sleep(1.1)  # timestamps are 1-second resolution
    p2 = archive_schedule(schedule, archive_dir=tmp_path)
    assert p1 != p2
