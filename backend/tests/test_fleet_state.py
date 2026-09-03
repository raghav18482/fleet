"""The seq guard: where at-least-once delivery becomes effectively-once fanout.

This is the trickiest part of the system and the part most likely to break silently, so
it gets the most coverage. A regression here does not raise -- it just makes robots
occasionally jump backwards on the operator's screen.
"""

from conftest import BASE_TS, telemetry

from app.health import STALE_AFTER_S


def test_roster_is_seeded_before_any_telemetry(fleet):
    # All eight (here, two) exist from boot. A consumer connecting early sees an
    # unaccounted-for fleet, not an empty one that fills in gradually.
    assert len(fleet.robots()) == 2
    r1 = fleet.get("r1")
    assert (r1.x, r1.y) == (10.0, 20.0)
    assert r1.status == "unknown"
    assert r1.stale is True
    assert r1.online is False
    assert fleet.version == 0


def test_first_reading_applies_and_bumps_version(fleet):
    updated = fleet.apply(telemetry(seq=1, x=111.0))

    assert updated is not None
    assert updated.x == 111.0
    assert updated.online is True  # arrival itself proves reachability
    assert updated.stale is False
    assert fleet.version == 1


def test_duplicate_seq_is_dropped(fleet):
    fleet.apply(telemetry(seq=7, x=111.0))
    version_after_first = fleet.version

    redelivered = fleet.apply(telemetry(seq=7, x=999.0))

    assert redelivered is None  # returns None, so nothing reaches the hub
    assert fleet.get("r1").x == 111.0
    assert fleet.version == version_after_first  # and the version does not move


def test_lower_seq_is_dropped(fleet):
    fleet.apply(telemetry(seq=42, x=420.0))

    assert fleet.apply(telemetry(seq=41, x=410.0)) is None
    assert fleet.get("r1").x == 420.0


def test_redelivery_does_not_move_a_robot_backwards(fleet):
    # The realistic failure: the broker's ack for seq 41 is lost, so it redelivers.
    # Arrival order becomes 40, 41, 42, 41.
    for seq, x in [(40, 400.0), (41, 410.0), (42, 420.0)]:
        assert fleet.apply(telemetry(seq=seq, x=x)) is not None

    assert fleet.apply(telemetry(seq=41, x=410.0)) is None
    assert fleet.get("r1").x == 420.0  # still at 42's position, not back at 41's


def test_higher_seq_after_a_gap_applies(fleet):
    # Messages genuinely lost in a gap must not wedge the robot -- only *older* readings
    # are refused, never newer ones.
    fleet.apply(telemetry(seq=1))
    updated = fleet.apply(telemetry(seq=500, x=555.0))

    assert updated is not None
    assert updated.x == 555.0


def test_seq_is_tracked_per_robot(fleet):
    fleet.apply(telemetry(robot_id="r1", seq=100))

    # r2's own sequence is independent; r1's high seq must not suppress it.
    assert fleet.apply(telemetry(robot_id="r2", seq=1)) is not None


def test_unknown_robot_id_is_ignored(fleet):
    assert fleet.apply(telemetry(robot_id="r99")) is None
    assert fleet.version == 0


def test_availability_change_bumps_version_once(fleet):
    fleet.apply(telemetry(seq=1))  # sets online=True
    version_before = fleet.version

    went_offline = fleet.set_availability("r1", False)
    assert went_offline is not None
    assert went_offline.online is False
    assert fleet.version == version_before + 1

    # A repeated retained message is not a change, so it must not fan out.
    assert fleet.set_availability("r1", False) is None
    assert fleet.version == version_before + 1


def test_sweep_marks_a_silent_robot_stale_exactly_once(fleet):
    fleet.apply(telemetry(seq=1, ts=BASE_TS))
    version_before = fleet.version

    assert fleet.sweep_stale(now=BASE_TS + 1) == []  # still fresh

    changed = fleet.sweep_stale(now=BASE_TS + STALE_AFTER_S + 1)
    assert [r.robot_id for r in changed] == ["r1"]
    assert fleet.get("r1").stale is True
    assert fleet.version == version_before + 1

    # Already stale: no repeated transition, no version churn, no repeated fanout.
    assert fleet.sweep_stale(now=BASE_TS + 999) == []
    assert fleet.version == version_before + 1


def test_never_seen_robot_does_not_generate_a_stale_transition(fleet):
    # r1 and r2 are seeded stale. Sweeping must not manufacture a transition for state
    # that was already true, or every consumer would get a pointless frame on boot.
    assert fleet.sweep_stale(now=BASE_TS + 9999) == []
    assert fleet.version == 0


def test_fresh_telemetry_clears_stale(fleet):
    fleet.apply(telemetry(seq=1, ts=BASE_TS))
    fleet.sweep_stale(now=BASE_TS + STALE_AFTER_S + 1)
    assert fleet.get("r1").stale is True

    recovered = fleet.apply(telemetry(seq=2, ts=BASE_TS + 100))
    assert recovered.stale is False


def test_snapshot_shape_matches_what_both_interfaces_send(fleet):
    fleet.apply(telemetry(seq=1))
    snap = fleet.snapshot(now=BASE_TS)

    assert snap["version"] == fleet.version
    assert len(snap["robots"]) == 2
    assert {"robot_id", "health", "stale", "online", "seq", "ts"} <= set(snap["robots"][0])


def test_state_objects_are_immutable_so_snapshots_do_not_drift(fleet):
    fleet.apply(telemetry(seq=1, x=100.0))
    captured = fleet.get("r1")

    fleet.apply(telemetry(seq=2, x=200.0))

    # The object a client is already holding must not change underneath it.
    assert captured.x == 100.0
    assert fleet.get("r1").x == 200.0
