"""Spawns and supervises one OS process per robot in robots.json.

This is the service the challenge asks for: "One of those services should run a script
that starts the robot simulation; we should not need to start it by hand in a separate
terminal."

Genuinely separate processes, not tasks in one interpreter -- each child has its own
memory, its own MQTT session, and can die independently without touching the other seven.
Restarting them here is what makes the chaos mode a real recovery cycle: a robot that
drops its connection is restarted, reconnects, and resumes publishing.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import time

ROBOTS_FILE = os.getenv("ROBOTS_FILE", "/data/robots.json")
RESTART_DELAY_S = float(os.getenv("RESTART_DELAY_S", "2"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [supervisor] %(message)s")
log = logging.getLogger("supervisor")

_running = True


def _stop(signum, frame):
    global _running
    _running = False


def spawn(robot_id):
    log.info("starting %s", robot_id)
    return subprocess.Popen(
        [sys.executable, "-u", os.path.join(os.path.dirname(__file__), "robot.py"), "--id", robot_id]
    )


def main():
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    with open(ROBOTS_FILE) as fh:
        roster = json.load(fh)

    robot_ids = [entry["robot_id"] for entry in roster]
    log.info("supervising %d robots: %s", len(robot_ids), ", ".join(robot_ids))

    children = {robot_id: spawn(robot_id) for robot_id in robot_ids}

    while _running:
        time.sleep(1)
        for robot_id, process in list(children.items()):
            code = process.poll()
            if code is None:
                continue
            log.warning("%s exited (code %s); restarting in %.0fs", robot_id, code, RESTART_DELAY_S)
            time.sleep(RESTART_DELAY_S)
            if _running:
                children[robot_id] = spawn(robot_id)

    log.info("stopping %d children", len(children))
    for process in children.values():
        process.terminate()
    for process in children.values():
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
    return 0


if __name__ == "__main__":
    sys.exit(main())
