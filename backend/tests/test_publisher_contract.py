"""The publisher/consumer contract, driven by the real events.jsonl.

sim/robot.py and backend/app/ingest.py are separate programs in separate containers that
never import each other -- their only shared surface is the JSON on the wire. Nothing else
in the test suite would notice if one renamed a field or changed a type; the mismatch
would surface as an empty dashboard at `docker compose up`.

So this drives actual recorded events through the actual publisher serializer, across a
bytes boundary, into the actual ingest handler.
"""

import json
from pathlib import Path

from robot import build_message, load_own_events

from app.config import load_roster
from app.health import STALE_AFTER_S
from app.hub import Hub
from app.ingest import handle_message
from app.models import serialize_robot
from app.reaper import reaper_tick
from app.state import FleetState

DATA = Path(__file__).resolve().parents[2] / "data"
EVENTS = str(DATA / "events.jsonl")


def wire(event, robot_id):
    """Exactly what leaves the publisher: a JSON document, as bytes."""
    return json.dumps(build_message(event, robot_id)).encode()


def fleet_from_real_roster():
    return FleetState(load_roster(DATA / "robots.json"))


def test_a_published_reading_is_understood_by_ingest():
    fleet = fleet_from_real_roster()
    event = load_own_events(EVENTS, "r6")[11]  # t=55, carries task_completed

    changed = handle_message(fleet, "fleet/robots/r6/telemetry", wire(event, "r6"))

    assert [r.robot_id for r in changed] == ["r6"]
    robot = fleet.get("r6")
    assert (robot.x, robot.y) == (event["x"], event["y"])
    assert robot.status == event["status"]
    assert robot.battery == event["battery"]
    assert robot.task_event == "task_completed"


def test_a_full_replay_of_one_robot_applies_every_reading():
    fleet = fleet_from_real_roster()
    events = load_own_events(EVENTS, "r3")

    applied = sum(
        len(handle_message(fleet, "fleet/robots/r3/telemetry", wire(e, "r3"))) for e in events
    )

    # Every reading must land. A silent drop here would look like a robot that freezes
    # partway through the window.
    assert applied == len(events) == 181
    assert fleet.get("r3").t == 900  # ended on the last recorded offset


def test_the_whole_fleet_replays_and_ends_healthy():
    fleet = fleet_from_real_roster()
    roster_ids = [entry["robot_id"] for entry in load_roster(DATA / "robots.json")]

    for robot_id in roster_ids:
        for event in load_own_events(EVENTS, robot_id):
            handle_message(fleet, "fleet/robots/%s/telemetry" % robot_id, wire(event, robot_id))

    assert len(fleet.robots()) == 8
    assert all(r.online and not r.stale for r in fleet.robots())
    assert all(r.seq > 0 for r in fleet.robots())


def test_every_status_in_the_log_survives_the_round_trip():
    """No recorded status may serialize to an unclassified verdict."""
    fleet = fleet_from_real_roster()
    seen = set()

    for robot_id in ["r%d" % n for n in range(1, 9)]:
        for event in load_own_events(EVENTS, robot_id):
            handle_message(fleet, "fleet/robots/%s/telemetry" % robot_id, wire(event, robot_id))
            seen.add(event["status"])

    assert len(seen) == 8  # all eight statuses the log contains appear
    for robot in fleet.robots():
        assert serialize_robot(robot, robot.ts)["health"] in {"working", "idle", "attention"}


def test_availability_and_telemetry_agree_on_robot_identity():
    # The two topics are parsed by the same code path; a divergence in how robot_id is
    # extracted would leave availability updating a robot that does not exist.
    fleet = fleet_from_real_roster()
    handle_message(fleet, "fleet/robots/r4/telemetry", wire(load_own_events(EVENTS, "r4")[0], "r4"))

    changed = handle_message(
        fleet, "fleet/robots/r4/availability", json.dumps({"online": False}).encode()
    )

    assert [r.robot_id for r in changed] == ["r4"]


def test_a_robot_that_stops_publishing_goes_stale_and_is_pushed():
    """The full failure path across both programs: publish, go quiet, get noticed."""
    fleet = fleet_from_real_roster()
    hub = Hub(fleet)
    connection, _ = hub.connect()

    event = load_own_events(EVENTS, "r7")[0]
    hub.publish(handle_message(fleet, "fleet/robots/r7/telemetry", wire(event, "r7")))
    assert "r7" in connection.pending
    connection.pending.clear()

    stale = reaper_tick(fleet, hub, now=fleet.get("r7").ts + STALE_AFTER_S + 1)

    assert [r.robot_id for r in stale] == ["r7"]
    assert connection.pending["r7"].stale is True
