#!/usr/bin/env python3
"""A WebSocket consumer for the fleet stream, and a REST/WebSocket consistency check.

Needs the `websockets` package. The zero-install way to run it is inside the backend
container, which already has it:

    docker compose exec -T backend python /srv/scripts/ws_client.py --check

Or locally, if you have websockets installed:

    python scripts/ws_client.py --frames 10
"""

import argparse
import asyncio
import json
import urllib.request

# Computed from wall-clock `now` at serialize time rather than from committed state, so
# two payloads describing the identical version can legitimately differ here. `ts` is the
# authoritative value and is compared.
RENDER_TIME_FIELDS = {"last_seen_s"}


def by_id(robots):
    return {
        r["robot_id"]: {k: v for k, v in r.items() if k not in RENDER_TIME_FIELDS}
        for r in robots
    }


def fetch_rest(base):
    with urllib.request.urlopen(base + "/robots") as response:
        return json.loads(response.read())


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost:8000")
    parser.add_argument("--frames", type=int, default=8, help="update frames to print")
    parser.add_argument(
        "--check",
        action="store_true",
        help="compare the opening snapshot against GET /robots",
    )
    args = parser.parse_args()

    import websockets

    ws_url = "ws://%s/ws/fleet" % args.host
    http_url = "http://%s" % args.host

    async with websockets.connect(ws_url) as ws:
        snapshot = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        print(
            "%-8s v%-6s %d robots"
            % (snapshot["type"], snapshot["version"], len(snapshot["robots"]))
        )

        if args.check:
            # Polled immediately after the snapshot frame. Live updates can land between
            # the two reads, so a mismatch is only meaningful if the versions agree.
            rest = fetch_rest(http_url)
            ws_robots, rest_robots = by_id(snapshot["robots"]), by_id(rest["robots"])
            agreed = sum(1 for k in ws_robots if ws_robots[k] == rest_robots.get(k))

            print("\n  ws version   : %s" % snapshot["version"])
            print("  rest version : %s" % rest["version"])
            print("  identical    : %d/%d robots" % (agreed, len(ws_robots)))
            if snapshot["version"] != rest["version"]:
                print(
                    "  (versions differ because %d update(s) landed between the two "
                    "reads -- lag, not disagreement)"
                    % (rest["version"] - snapshot["version"])
                )
            print()

        for _ in range(args.frames):
            frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
            changed = ", ".join(
                "%s=%s" % (r["robot_id"], r["health"]) for r in frame["robots"]
            )
            print("%-8s v%-6s %s" % (frame["type"], frame["version"], changed))


if __name__ == "__main__":
    asyncio.run(main())
