"""Environment-driven configuration, with defaults that work both in the container and
on a developer machine running the tests directly."""

import json
import os
from pathlib import Path


def _default_data_dir():
    # The compose services mount the challenge data at /data. Falling back to the repo's
    # own data/ keeps `pytest` runnable on the host with no environment set up.
    if Path("/data/robots.json").exists():
        return Path("/data")
    return Path(__file__).resolve().parents[2] / "data"


DATA_DIR = Path(os.getenv("DATA_DIR", str(_default_data_dir())))
ROBOTS_FILE = DATA_DIR / "robots.json"
EVENTS_FILE = DATA_DIR / "events.jsonl"

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "fleet/robots")
TELEMETRY_FILTER = TOPIC_PREFIX + "/+/telemetry"
AVAILABILITY_FILTER = TOPIC_PREFIX + "/+/availability"


def telemetry_topic(robot_id):
    return "{}/{}/telemetry".format(TOPIC_PREFIX, robot_id)


def availability_topic(robot_id):
    return "{}/{}/availability".format(TOPIC_PREFIX, robot_id)


# How often the reaper sweeps for robots that have gone quiet. Well below
# health.STALE_AFTER_S so the transition lands promptly rather than up to a full
# threshold late.
REAPER_INTERVAL_S = float(os.getenv("REAPER_INTERVAL_S", "2.0"))

DB_PATH = os.getenv("DB_PATH", "/tmp/fleet_history.db")
HISTORY_FLUSH_S = float(os.getenv("HISTORY_FLUSH_S", "1.0"))


def load_roster(path=None):
    with open(str(path or ROBOTS_FILE)) as fh:
        return json.load(fh)
