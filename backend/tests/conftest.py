import pytest

from app.models import Telemetry
from app.state import FleetState

# A two-robot stand-in for robots.json. Small enough to reason about, same shape.
ROSTER = [
    {"robot_id": "r1", "robot_type": "picker", "start": {"x": 10.0, "y": 20.0}},
    {"robot_id": "r2", "robot_type": "hauler", "start": {"x": 30.0, "y": 40.0}},
]

BASE_TS = 1_700_000_000.0


@pytest.fixture
def fleet():
    return FleetState(ROSTER)


def telemetry(
    robot_id="r1",
    seq=1,
    ts=None,
    t=0,
    x=100.0,
    y=200.0,
    status="active",
    battery=80.0,
    task_event=None,
):
    """Build a reading. Defaults are a healthy, working robot."""
    return Telemetry(
        robot_id=robot_id,
        seq=seq,
        ts=BASE_TS if ts is None else ts,
        t=t,
        x=x,
        y=y,
        status=status,
        battery=battery,
        task_event=task_event,
    )
