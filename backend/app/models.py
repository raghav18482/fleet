"""Wire format and in-memory shape for a single robot.

Two types, deliberately separate:

  Telemetry   what a robot publisher puts on the wire. Validated on arrival.
  RobotState  what the backend holds. Frozen, so a snapshot taken now cannot be
              mutated out from under a client by an update that lands later.
"""

from dataclasses import dataclass, replace
from typing import Optional

from pydantic import BaseModel

from .health import classify

# Applied before we have heard anything at all from a robot in the roster.
UNKNOWN_STATUS = "unknown"


class Telemetry(BaseModel):
    """One reading, as published by a robot.

    `seq` and `ts` are added by the publisher; they are not in events.jsonl. `t` is the
    recorded replay offset and is carried through purely as provenance -- it proves the
    feed is sourced from the supplied log rather than invented, and it resets to 0 every
    time a publisher loops, which is exactly why it cannot be used as a timestamp.
    """

    robot_id: str
    seq: int
    ts: float
    t: int
    x: float
    y: float
    status: str
    battery: float
    task_event: Optional[str] = None


@dataclass(frozen=True)
class RobotState:
    """Current known state of one robot.

    Frozen on purpose. FleetState replaces the object rather than mutating it, so a
    snapshot handed to a REST response and an entry sitting in a WebSocket client's
    pending buffer both keep the values they had when the version was stamped.
    """

    robot_id: str
    robot_type: str
    x: float
    y: float
    seq: int = -1
    ts: float = 0.0
    t: int = 0
    status: str = UNKNOWN_STATUS
    battery: float = 0.0
    task_event: Optional[str] = None

    # Reachability, from the availability topic (the broker publishes the robot's Last
    # Will here when its connection dies) and from telemetry simply arriving.
    online: bool = False

    # No telemetry within STALE_AFTER_S. Owned by FleetState, fired by the reaper.
    # Seeded True: a robot we have never heard from is not healthy, it is unaccounted for.
    stale: bool = True

    def with_telemetry(self, t):
        """Return a new state carrying this reading. Arrival itself proves reachability."""
        return replace(
            self,
            seq=t.seq,
            ts=t.ts,
            t=t.t,
            x=t.x,
            y=t.y,
            status=t.status,
            battery=t.battery,
            task_event=t.task_event,
            online=True,
            stale=False,
        )

    def health(self):
        return classify(self.status, self.battery, self.stale, self.online)


def serialize_robot(r, now):
    """The single serializer. Both `GET /robots` and the WebSocket frames call this.

    There is exactly one of these by design -- two would drift apart the first time
    somebody added a field to one path and forgot the other, and the drift would show up
    as a REST client and a WS client disagreeing.

    `health` and `stale` are versioned state: they change only when FleetState changes,
    so both interfaces always report the same verdict for the same version.
    `last_seen_s` is a render-time convenience computed from `now` and is not versioned;
    `ts` is the authoritative value a client should derive its own age from.
    """
    return {
        "robot_id": r.robot_id,
        "robot_type": r.robot_type,
        "x": r.x,
        "y": r.y,
        "status": r.status,
        "battery": r.battery,
        "health": r.health().value,
        "online": r.online,
        "stale": r.stale,
        "last_seen_s": None if r.seq < 0 else round(now - r.ts, 1),
        "task_event": r.task_event,
        "seq": r.seq,
        "ts": r.ts,
        "t": r.t,
    }
