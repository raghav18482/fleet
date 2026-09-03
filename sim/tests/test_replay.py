"""The publisher's replay logic, against the real events.jsonl.

Runs on the actual challenge data rather than a fixture, because the properties that
matter here are properties of that file: that each robot's slice is complete and ordered,
and that what goes on the wire is derived from the recording rather than invented.
"""

import json
from pathlib import Path

import pytest

from robot import build_message, load_own_events

DATA = Path(__file__).resolve().parents[2] / "data"
EVENTS = DATA / "events.jsonl"
ROBOTS = DATA / "robots.json"

ROSTER_IDS = [entry["robot_id"] for entry in json.loads(ROBOTS.read_text())]


def test_roster_is_the_expected_eight():
    assert ROSTER_IDS == ["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"]


@pytest.mark.parametrize("robot_id", ROSTER_IDS)
def test_each_robot_owns_a_complete_ordered_slice(robot_id):
    events = load_own_events(str(EVENTS), robot_id)

    assert len(events) == 181  # 0..900s inclusive at 5s spacing
    assert all(e["robot_id"] == robot_id for e in events)
    assert [e["t"] for e in events] == sorted(e["t"] for e in events)
    assert events[0]["t"] == 0 and events[-1]["t"] == 900


def test_slices_partition_the_log_without_overlap():
    total = sum(len(load_own_events(str(EVENTS), rid)) for rid in ROSTER_IDS)
    assert total == sum(1 for line in EVENTS.open() if line.strip())


def test_message_carries_the_recorded_values_unchanged():
    event = load_own_events(str(EVENTS), "r6")[11]  # t=55, the task_completed row
    message = build_message(event, "r6")

    for field in ("x", "y", "status", "battery", "t"):
        assert message[field] == event[field]
    assert message["robot_id"] == "r6"
    assert message["task_event"] == "task_completed"


def test_message_adds_a_monotonic_publication_token():
    events = load_own_events(str(EVENTS), "r1")

    first = build_message(events[0], "r1")
    second = build_message(events[1], "r1")

    # seq must increase for the backend's guard to accept the second reading. It is
    # derived from the wall clock rather than a counter so that a crashed-and-restarted
    # publisher does not resume at 0 and get everything it sends rejected.
    assert second["seq"] > first["seq"]
    assert second["ts"] >= first["ts"]


def test_replay_offset_is_not_used_as_a_timestamp():
    # `t` restarts at 0 on every loop of the recording, so it can only ever be
    # provenance. Ordering has to come from seq.
    events = load_own_events(str(EVENTS), "r1")
    assert build_message(events[0], "r1")["t"] == 0
    assert build_message(events[0], "r1")["ts"] > 1_600_000_000


def test_absent_task_event_is_explicit_null():
    event = load_own_events(str(EVENTS), "r1")[0]
    assert build_message(event, "r1")["task_event"] is None
