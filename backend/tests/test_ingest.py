"""Message decoding. Pure function, so no broker is involved in testing the logic that
decides what a message means."""

import json

from conftest import telemetry

from app.ingest import handle_message


def payload(t):
    return t.model_dump_json().encode()


def test_telemetry_is_applied_and_returned(fleet):
    changed = handle_message(fleet, "fleet/robots/r1/telemetry", payload(telemetry(seq=1, x=42.0)))

    assert [r.robot_id for r in changed] == ["r1"]
    assert fleet.get("r1").x == 42.0


def test_duplicate_telemetry_produces_no_change(fleet):
    handle_message(fleet, "fleet/robots/r1/telemetry", payload(telemetry(seq=5)))

    # A QoS 1 redelivery. Returning [] is what stops it reaching the hub or the history
    # writer -- the empty list here is the whole point, not an error.
    assert handle_message(fleet, "fleet/robots/r1/telemetry", payload(telemetry(seq=5))) == []


def test_availability_offline_marks_the_robot_unreachable(fleet):
    handle_message(fleet, "fleet/robots/r1/telemetry", payload(telemetry(seq=1)))

    # What the broker publishes on the robot's behalf when its connection dies.
    changed = handle_message(
        fleet, "fleet/robots/r1/availability", json.dumps({"online": False}).encode()
    )

    assert [r.robot_id for r in changed] == ["r1"]
    assert fleet.get("r1").online is False
    assert fleet.get("r1").health().value == "attention"


def test_repeated_availability_does_not_refan(fleet):
    handle_message(fleet, "fleet/robots/r1/availability", b'{"online": false}')
    # Retained messages are redelivered on every resubscribe; an unchanged value must not
    # generate a frame for every connected client each time we reconnect.
    assert handle_message(fleet, "fleet/robots/r1/availability", b'{"online": false}') == []


def test_malformed_payload_is_discarded_not_raised(fleet):
    # One robot publishing garbage must not take ingest down for the other seven.
    assert handle_message(fleet, "fleet/robots/r1/telemetry", b"not json at all") == []
    assert fleet.version == 0


def test_unknown_topic_suffix_is_ignored(fleet):
    assert handle_message(fleet, "fleet/robots/r1/somethingelse", b"{}") == []


def test_short_topic_is_ignored(fleet):
    assert handle_message(fleet, "telemetry", b"{}") == []
