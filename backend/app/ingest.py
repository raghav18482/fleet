"""Consumes the robot feed from MQTT and applies it to shared state.

Split deliberately in two:

  handle_message()  pure. Decode one message, apply it, return what changed. No I/O, so
                    the interesting logic is testable without a broker.
  run_ingest()      the connection loop. Subscribes, and reconnects with backoff when the
                    broker or the network goes away.

Why MQTT at all: it decouples the eight publishers from this consumer entirely — a robot
neither knows nor cares whether a backend is listening. QoS 1 gives at-least-once
delivery, `clean_session=False` makes the broker queue messages for us while we are
restarting instead of dropping them, and retained messages mean a cold start receives
every robot's last known reading immediately rather than sitting blind until the next tick.

The cost of at-least-once is duplicates and redelivered stragglers. Those are absorbed by
the seq guard in FleetState.apply(), which is the single place this system reconciles
at-least-once transport with exactly-once fanout.
"""

import asyncio
import json
import logging

import aiomqtt

from .config import (
    AVAILABILITY_FILTER,
    MQTT_HOST,
    MQTT_PORT,
    TELEMETRY_FILTER,
)
from .models import Telemetry

log = logging.getLogger(__name__)

BACKOFF_START_S = 1.0
BACKOFF_MAX_S = 30.0

# Fixed so the broker recognises us across restarts and can hold our queued session.
CLIENT_ID = "fleet-backend"


def handle_message(state, topic, payload, history=None):
    """Decode and apply one message. Returns the robots that actually changed.

    An empty list is the normal, expected outcome for a duplicate — not an error.

    History is recorded here rather than from the caller's view of what changed, and the
    difference shows up after a reconnect: the broker delivers its queued backlog, and
    only the newest reading in it advances current state. Recording only what apply()
    accepted would discard every backfilled point, leaving the history of a disruption
    empty exactly when someone wanted to look at it. So every *valid* reading is
    recorded, and the primary key on (robot_id, seq) makes redeliveries idempotent.
    """
    parts = str(topic).split("/")
    if len(parts) < 2:
        return []
    robot_id, kind = parts[-2], parts[-1]

    try:
        if kind == "telemetry":
            reading = Telemetry.model_validate_json(payload)
            if history is not None:
                history.record(reading)
            updated = state.apply(reading)
        elif kind == "availability":
            online = json.loads(payload).get("online", False)
            updated = state.set_availability(robot_id, online)
        else:
            return []
    except Exception:
        # One malformed publisher must not take down ingest for the other seven.
        log.warning("discarding unparseable message on %s", topic, exc_info=True)
        return []

    return [updated] if updated is not None else []


async def run_ingest(state, hub, history=None, host=MQTT_HOST, port=MQTT_PORT):
    """Subscribe and pump messages forever, reconnecting on failure.

    Real deployments drop connections, so this loop treats disconnection as routine
    rather than fatal. Backoff is exponential and capped, so a broker that is down for
    ten minutes does not turn into a reconnect storm the moment it returns.
    """
    backoff = BACKOFF_START_S

    while True:
        try:
            async with aiomqtt.Client(
                hostname=host,
                port=port,
                identifier=CLIENT_ID,
                clean_session=False,  # broker queues our QoS 1 messages while we are away
                keepalive=15,
            ) as client:
                log.info("connected to broker at %s:%s", host, port)
                backoff = BACKOFF_START_S  # only reset after a *successful* connect

                await client.subscribe(TELEMETRY_FILTER, qos=1)
                await client.subscribe(AVAILABILITY_FILTER, qos=1)

                async for message in client.messages:
                    hub.publish(handle_message(state, message.topic, message.payload, history))

        except aiomqtt.MqttError as exc:
            log.warning("broker connection lost (%s); retrying in %.0fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_S)
