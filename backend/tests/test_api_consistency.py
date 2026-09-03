"""The headline requirement: "a client using one should not see something inconsistent
with a client using the other."

What is asserted here is the precise version of that claim -- everything derived from
committed state is identical across both interfaces, and the one field that is not
derived from committed state is identified as such rather than quietly excluded.
"""

import asyncio

import pytest
from conftest import BASE_TS, ROSTER, telemetry
from fastapi.testclient import TestClient

from app.health import STALE_AFTER_S
from app.hub import Hub
from app.main import create_app
from app.models import serialize_robot
from app.reaper import reaper_tick
from app.state import FleetState

# `last_seen_s` is a convenience readout computed from wall-clock `now` at serialize
# time, not versioned state. Two payloads built microseconds apart can legitimately
# differ in it while describing the identical committed state; `ts` is the authoritative
# value and IS compared.
RENDER_TIME_FIELDS = {"last_seen_s"}


def versioned(robot):
    return {k: v for k, v in robot.items() if k not in RENDER_TIME_FIELDS}


def build(fleet):
    hub = Hub(fleet)
    return TestClient(create_app(state=fleet, hub=hub, start_tasks=False)), hub


def test_websocket_snapshot_is_identical_to_the_rest_response(fleet):
    client, _ = build(fleet)
    fleet.apply(telemetry(robot_id="r1", seq=1, status="error", battery=9.0))
    fleet.apply(telemetry(robot_id="r2", seq=1, status="on_mission"))

    with client.websocket_connect("/ws/fleet") as ws:
        frame = ws.receive_json()
    rest = client.get("/robots").json()

    assert frame["type"] == "snapshot"
    assert frame["version"] == rest["version"]
    assert [versioned(r) for r in frame["robots"]] == [versioned(r) for r in rest["robots"]]


def test_both_interfaces_report_the_same_version_as_state_advances(fleet):
    client, _ = build(fleet)

    for seq in range(1, 26):
        fleet.apply(telemetry(seq=seq))

    with client.websocket_connect("/ws/fleet") as ws:
        frame = ws.receive_json()

    assert frame["version"] == client.get("/robots").json()["version"] == fleet.version


async def test_reaper_transition_is_pushed_not_merely_inferred(fleet):
    """The regression test for the subtlest way these two views can drift apart.

    Staleness comes from elapsed time, not from an arriving message. A poller computing
    it per-request would flag a silent robot on its own while a subscriber, hearing
    nothing, showed it as healthy indefinitely -- with no bug in the transport, the state
    or the serializer.

    Asserting only that REST changed would pass even in that broken world. Draining the
    subscriber is what proves the transition was actually fanned out: if the reaper
    stopped publishing, this call would block until the timeout rather than fail loudly.
    """
    hub = Hub(fleet)
    connection, _ = hub.connect()

    hub.publish([fleet.apply(telemetry(seq=1, ts=BASE_TS))])
    await asyncio.wait_for(connection.drain(), timeout=1)  # clear the initial update

    reaper_tick(fleet, hub, now=BASE_TS + STALE_AFTER_S + 1)

    batch = await asyncio.wait_for(connection.drain(), timeout=1)

    assert [r.robot_id for r in batch] == ["r1"]
    assert batch[0].stale is True
    assert serialize_robot(batch[0], BASE_TS)["health"] == "attention"


def test_rest_agrees_with_the_pushed_staleness(fleet):
    client, hub = build(fleet)
    fleet.apply(telemetry(seq=1, ts=BASE_TS))
    reaper_tick(fleet, hub, now=BASE_TS + STALE_AFTER_S + 1)

    r1 = [r for r in client.get("/robots").json()["robots"] if r["robot_id"] == "r1"][0]

    assert r1["stale"] is True
    assert r1["health"] == "attention"


def test_health_filter_selects_robots_needing_attention(fleet):
    client, _ = build(fleet)
    fleet.apply(telemetry(robot_id="r1", seq=1, status="error"))
    fleet.apply(telemetry(robot_id="r2", seq=1, status="on_mission", battery=90.0))

    attention = client.get("/robots", params={"health": "attention"}).json()

    assert [r["robot_id"] for r in attention["robots"]] == ["r1"]
    assert attention["version"] == fleet.version  # a filtered view is still a versioned view


def test_status_filter_uses_the_raw_reported_status(fleet):
    client, _ = build(fleet)
    fleet.apply(telemetry(robot_id="r1", seq=1, status="blocked"))
    fleet.apply(telemetry(robot_id="r2", seq=1, status="idle"))

    blocked = client.get("/robots", params={"status": "blocked"}).json()

    assert [r["robot_id"] for r in blocked["robots"]] == ["r1"]


def test_single_robot_endpoint_matches_the_list_entry(fleet):
    client, _ = build(fleet)
    fleet.apply(telemetry(robot_id="r1", seq=1, status="charging"))

    one = client.get("/robots/r1").json()
    from_list = [r for r in client.get("/robots").json()["robots"] if r["robot_id"] == "r1"][0]

    assert versioned(one["robot"]) == versioned(from_list)


def test_unknown_robot_is_404(fleet):
    client, _ = build(fleet)
    assert client.get("/robots/r99").status_code == 404


def test_summary_counts_match_the_fleet(fleet):
    client, _ = build(fleet)
    fleet.apply(telemetry(robot_id="r1", seq=1, status="error"))
    fleet.apply(telemetry(robot_id="r2", seq=1, status="on_mission", battery=90.0))

    summary = client.get("/fleet/summary").json()

    assert summary["total"] == 2
    assert summary["by_status"] == {"error": 1, "on_mission": 1}
    assert summary["by_health"] == {"attention": 1, "working": 1}


def test_full_roster_is_visible_before_any_telemetry():
    # A consumer that connects at boot must see eight unaccounted-for robots, not an
    # empty fleet -- the latter is indistinguishable from the fleet having vanished.
    client, _ = build(FleetState(ROSTER))

    body = client.get("/robots").json()

    assert body["version"] == 0
    assert len(body["robots"]) == 2
    assert all(r["health"] == "attention" and r["stale"] for r in body["robots"])


@pytest.mark.parametrize("path", ["/health", "/robots", "/fleet/summary"])
def test_endpoints_respond_without_a_broker(fleet, path):
    # Nothing in the read path may depend on MQTT being reachable.
    client, _ = build(fleet)
    assert client.get(path).status_code == 200


def test_every_route_handler_runs_on_the_event_loop(fleet):
    """No route may be a plain `def`, because FastAPI would run it in a threadpool.

    A handler reading FleetState from a worker thread while ingest mutates it on the
    event loop can observe a torn read -- `version` from one instant and robot data from
    another -- which is exactly the poller/subscriber inconsistency this design claims to
    rule out. Nothing raises when that happens; the numbers are just quietly wrong.

    Adding a route without `async` is an easy regression, so the invariant is asserted
    directly rather than left to review.
    """
    app = create_app(state=fleet, hub=Hub(fleet), start_tasks=False)

    sync_routes = [
        route.path
        for route in app.routes
        if hasattr(route, "endpoint")
        and getattr(route, "methods", None)
        and not asyncio.iscoroutinefunction(route.endpoint)
    ]

    assert sync_routes == []
