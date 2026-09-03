"""The status -> operator-verdict mapping, and the three overrides that beat it.

These assertions encode a judgement call the challenge left open on purpose, so they
double as the written record of what was decided.
"""

import pytest

from app.health import LOW_BATTERY_PCT, STATUS_CLASS, Health, classify


def working(status, battery=80.0):
    return classify(status, battery, stale=False, online=True)


@pytest.mark.parametrize(
    "status,expected",
    [
        ("active", Health.WORKING),
        ("on_mission", Health.WORKING),
        ("charging", Health.WORKING),  # healthy and needs nobody
        ("idle", Health.IDLE),
        ("maintenance", Health.IDLE),  # planned downtime is already known
        ("blocked", Health.ATTENTION),
        ("error", Health.ATTENTION),
        ("offline", Health.ATTENTION),
        ("unknown", Health.ATTENTION),  # silence is not health
    ],
)
def test_status_maps_to_expected_verdict(status, expected):
    assert working(status) == expected


def test_every_status_in_the_recorded_log_is_classified():
    # The eight statuses events.jsonl actually contains. An unmapped one would silently
    # fall through to ATTENTION and quietly pollute the operator's attention list.
    recorded = {
        "idle",
        "active",
        "on_mission",
        "charging",
        "blocked",
        "error",
        "maintenance",
        "offline",
    }
    assert recorded <= set(STATUS_CLASS)


def test_low_battery_overrides_a_working_status():
    assert working("on_mission", battery=LOW_BATTERY_PCT - 0.1) == Health.ATTENTION
    assert working("on_mission", battery=LOW_BATTERY_PCT + 0.1) == Health.WORKING


def test_low_battery_while_charging_is_still_attention():
    # Defensible either way; the call is that a nearly-flat robot is worth surfacing even
    # though it is already doing the right thing about it.
    assert working("charging", battery=5.0) == Health.ATTENTION


def test_stale_overrides_everything():
    assert classify("on_mission", 100.0, stale=True, online=True) == Health.ATTENTION


def test_offline_overrides_everything():
    assert classify("on_mission", 100.0, stale=False, online=False) == Health.ATTENTION


def test_unrecognised_status_fails_safe_to_attention():
    # A robot reporting something we have never seen is a reason to look, not to ignore.
    assert working("teleporting") == Health.ATTENTION
