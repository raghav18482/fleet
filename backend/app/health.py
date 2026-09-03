"""How a robot's reported status maps to what an operator should do about it.

The challenge deliberately leaves this undefined: "We deliberately do not define which
statuses count as working or as needing attention, or what active means versus
on_mission. Make a sensible call and be ready to defend it."

Everything defensible lives in this one file so the call is easy to state and easy to
change. The thresholds below are the only tunables.
"""

from enum import Enum


class Health(str, Enum):
    """What the operator should do, as opposed to what the robot is doing."""

    WORKING = "working"
    IDLE = "idle"
    ATTENTION = "attention"


# The defended call:
#
#   active / on_mission  Both are working. on_mission is executing an assigned task;
#                        active is powered movement without one (repositioning,
#                        returning to dock). The distinction matters to a scheduler,
#                        not to an operator deciding where to look.
#   charging             Working. A charging robot is healthy and needs nobody. Flagging
#                        it would train operators to ignore the attention list.
#   maintenance          Idle, not attention. Planned downtime is already known.
#   blocked / error      Attention. Something is in the way or broken.
#   offline              Attention. The robot reports itself unavailable.
#   unknown              Attention. Seeded state for a robot we have never heard from —
#                        silence is not the same as health.
STATUS_CLASS = {
    "active": Health.WORKING,
    "on_mission": Health.WORKING,
    "charging": Health.WORKING,
    "idle": Health.IDLE,
    "maintenance": Health.IDLE,
    "blocked": Health.ATTENTION,
    "error": Health.ATTENTION,
    "offline": Health.ATTENTION,
    "unknown": Health.ATTENTION,
}

LOW_BATTERY_PCT = 20.0

# 3x the 5s reporting cadence: one missed report is jitter, three is a problem.
STALE_AFTER_S = 15.0


def classify(status, battery, stale, online):
    """Reduce a robot's reported state to a single operator-facing verdict.

    `stale` is passed in as a stored boolean rather than derived from a clock here. That
    is deliberate: a value derived from `now` would be recomputed on every REST request
    while the WebSocket stream heard nothing, and the two interfaces would silently
    disagree. FleetState owns the transition, and the reaper is what fires it.
    """
    if stale or not online:
        return Health.ATTENTION
    if battery < LOW_BATTERY_PCT:
        return Health.ATTENTION
    return STATUS_CLASS.get(status, Health.ATTENTION)
