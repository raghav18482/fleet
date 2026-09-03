"""Detects robots that have gone quiet, and — just as importantly — makes that a versioned
event rather than something each interface works out for itself.

There are two independent ways this system notices a robot is gone, and neither alone is
enough:

  MQTT Last Will      The broker publishes the robot's will when its TCP connection dies.
                      Fast and protocol-level, but blind to a robot that is still
                      connected and simply stopped saying anything.

  This reaper         Catches alive-but-mute: wedged process, partitioned network, robot
                      powered but not publishing. Slower, but it cannot be fooled by a
                      socket that happens to stay open.

The consistency role matters as much as the alerting one. Staleness is derived from
elapsed time, so a polling client that computed it at request time would flip a silent
robot to attention entirely on its own, while a WebSocket subscriber heard nothing —
silence produces no message to push. Committing the transition into shared state here,
with a version bump and a fanout, is what keeps the two interfaces agreeing.
"""

import asyncio
import logging

from .config import REAPER_INTERVAL_S

log = logging.getLogger(__name__)


def reaper_tick(state, hub, now=None):
    """One sweep. Split out from the loop so tests can drive it without a clock."""
    changed = state.sweep_stale(now)
    hub.publish(changed)
    if changed:
        log.info("marked stale: %s", ", ".join(r.robot_id for r in changed))
    return changed


async def run_reaper(state, hub, interval=REAPER_INTERVAL_S):
    while True:
        await asyncio.sleep(interval)
        try:
            reaper_tick(state, hub)
        except Exception:
            # A failed sweep must not kill the task; the next tick retries in `interval`.
            log.exception("reaper sweep failed")
