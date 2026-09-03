"""The fleet's current state: one dict, one process, one source of truth.

This is the answer to "both need to reflect the same underlying state; a client using one
should not see something inconsistent with a client using the other." The REST handlers
and the WebSocket hub read *this same object* on the same asyncio event loop, so
consistency is structural rather than coordinated -- there is no sync step between them
because there are not two copies to sync.

That holds for exactly one backend replica. Where it stops holding, and what replaces it,
is covered in SYSTEM_DESIGN.md.
"""

import time
from dataclasses import replace
from typing import Dict, List, Optional

from .health import STALE_AFTER_S
from .models import RobotState, serialize_robot


class FleetState:
    def __init__(self, roster):
        """Seed every robot in robots.json up front.

        The fleet is a fixed roster, so all eight exist from boot with their recorded
        start position, `status="unknown"` and `stale=True`. A consumer that connects
        before any telemetry arrives sees eight unaccounted-for robots rather than an
        empty fleet that fills in one at a time -- the second is indistinguishable from
        six robots having vanished.
        """
        self._robots: Dict[str, RobotState] = {}
        self._last_seq: Dict[str, int] = {}
        self._version = 0

        for entry in roster:
            robot_id = entry["robot_id"]
            self._robots[robot_id] = RobotState(
                robot_id=robot_id,
                robot_type=entry["robot_type"],
                x=entry["start"]["x"],
                y=entry["start"]["y"],
            )
            self._last_seq[robot_id] = -1

    @property
    def version(self):
        """Bumped on every accepted mutation. Stamped on every payload, both interfaces."""
        return self._version

    def apply(self, telemetry) -> Optional[RobotState]:
        """Apply a reading, or return None if it should not be applied.

        This is where MQTT's at-least-once delivery becomes effectively-once fanout.

        QoS 1 means the broker redelivers anything it did not get an ack for, so
        duplicates are expected rather than exceptional, and a delayed message can arrive
        after a newer one. Both are handled by the same comparison: anything at or below
        the highest seq already seen for this robot is dropped here and never reaches the
        hub, the history writer, or the version counter.

        Without this guard a redelivered older reading would move a robot *backwards* on
        the operator's screen, to a position it had already left.
        """
        known = self._robots.get(telemetry.robot_id)
        if known is None:
            return None  # not in the roster; nothing legitimate publishes as an unknown id

        if telemetry.seq <= self._last_seq[telemetry.robot_id]:
            return None  # duplicate redelivery, or a straggler overtaken by a newer reading

        self._last_seq[telemetry.robot_id] = telemetry.seq
        updated = known.with_telemetry(telemetry)
        self._robots[telemetry.robot_id] = updated
        self._version += 1
        return updated

    def set_availability(self, robot_id, online) -> Optional[RobotState]:
        """Apply a reachability change from the availability topic.

        `online=False` is typically the broker publishing the robot's Last Will after its
        connection dropped -- the robot announcing its own death by having arranged for it
        in advance. Returns None when nothing actually changed, so a repeated retained
        message does not produce a spurious fanout.
        """
        known = self._robots.get(robot_id)
        if known is None or known.online == online:
            return None

        updated = replace(known, online=online)
        self._robots[robot_id] = updated
        self._version += 1
        return updated

    def sweep_stale(self, now=None) -> List[RobotState]:
        """Flip robots that have gone quiet to stale. Returns only what changed.

        Staleness is derived from elapsed time, not from an arriving message, which makes
        it the one piece of state that can diverge between a polling client and a
        streaming one for free: a poller computing it at request time would flip a silent
        robot to attention on its own, while a subscriber heard nothing at all, because
        silence produces no message to push.

        Committing the transition here -- into the shared state, with a version bump, and
        fanned out by the caller -- is what makes it an event both interfaces observe.
        """
        now = time.time() if now is None else now
        changed = []

        for robot_id, robot in self._robots.items():
            if robot.seq < 0:
                continue  # never heard from; seeded stale already, nothing to transition
            if not robot.stale and (now - robot.ts) > STALE_AFTER_S:
                updated = replace(robot, stale=True)
                self._robots[robot_id] = updated
                self._version += 1
                changed.append(updated)

        return changed

    def get(self, robot_id) -> Optional[RobotState]:
        return self._robots.get(robot_id)

    def robots(self) -> List[RobotState]:
        return list(self._robots.values())

    def snapshot(self, now=None) -> dict:
        """The full fleet, in the exact shape both interfaces send it."""
        now = time.time() if now is None else now
        return {
            "version": self._version,
            "robots": [serialize_robot(r, now) for r in self._robots.values()],
        }
