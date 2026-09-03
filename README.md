# Fleet management backend 

Eight simulated robots, each its own OS process, replay their
slice of `events.jsonl` over MQTT. A FastAPI service consumes that feed, maintains the
fleet's current state, and exposes it two ways — a WebSocket stream and a REST endpoint —
that cannot disagree with each other.

## Run it

```bash
docker compose up --build
```

That is the whole setup. Three services come up: the broker, the backend, and the
supervisor that forks eight robot publishers. Nothing needs starting by hand.

The recorded fifteen-minute window replays at 10x by default, so the full window plays out
in about ninety seconds and then loops. Change it with `SPEED=1 docker compose up`.

### Polling it

```bash
curl -s localhost:8000/health | jq

# the whole fleet, one line per robot
curl -s localhost:8000/robots | jq -r '.robots[] |
  "\(.robot_id)  \(.robot_type)  bat=\(.battery)%  \(.status) -> \(.health)"'

curl -s "localhost:8000/robots?health=attention" | jq -r '.robots[].robot_id'
curl -s "localhost:8000/robots?status=blocked" | jq -r '.robots[].robot_id'
curl -s localhost:8000/robots/r4 | jq
curl -s localhost:8000/fleet/summary | jq
curl -s "localhost:8000/robots/history/r4?limit=5" | jq '.points'
```

### Streaming it

`scripts/ws_client.py` is mounted into the backend container, which already has
`websockets` — so this needs nothing installed locally:

```bash
docker compose exec -T backend python /srv/scripts/ws_client.py --check --frames 10
```

`--check` polls `GET /robots` immediately after receiving the opening WebSocket snapshot
and compares them, which is the requirement made observable:

```
snapshot v176    8 robots
  ws version   : 176
  rest version : 176
  identical    : 8/8 robots
update   v177    r3=working
update   v178    r4=idle
```

If the versions differ it is because updates landed between the two reads — lag, not
disagreement. The script says so explicitly rather than reporting a failure.

Run it against the host instead with `python scripts/ws_client.py --check` (needs
`pip install websockets`), or from a browser console at `localhost:8000/docs`:

```js
const ws = new WebSocket("ws://localhost:8000/ws/fleet");
ws.onmessage = e => { const f = JSON.parse(e.data);
  console.log(f.type, "v" + f.version, f.robots.map(r => r.robot_id)); };
```

## Endpoints

| Method | Path | Notes |
|---|---|---|
| GET | `/health` | liveness, used by the container healthcheck |
| GET | `/robots` | full fleet; `?health=working\|idle\|attention`, `?status=error` |
| GET | `/robots/{id}` | one robot, 404 if not in the roster |
| GET | `/fleet/summary` | counts by status and by health |
| GET | `/robots/history/{id}` | `?from=&to=&limit=` — the optional stretch goal |
| WS | `/ws/fleet` | snapshot frame on connect, then coalesced deltas |

Interactive docs at `localhost:8000/docs`.

## Design decisions

**MQTT (Mosquitto) for robot → backend.** The robots and the backend never address each
other; publishers write to `fleet/robots/{id}/telemetry` and the backend subscribes to
`fleet/robots/+/telemetry`. Three broker features do real work rather than being
decoration:

- **QoS 1** gives at-least-once delivery, so a lost ack means redelivery rather than a
  lost reading. The cost — duplicates — is absorbed in one place (below).
- **`clean_session=False`** on both sides means the broker holds each session, so messages
  published while the backend is restarting are queued rather than dropped.
- **Last Will and Testament** is how the system learns about a hard failure. Each robot
  registers a will at connect time; when its connection dies the *broker* publishes
  `online: false` on its behalf. The robot announces its own death by arranging it in
  advance. See `sim/robot.py::connect`.

It is also what real robot fleets speak, which matters for a system meant to stand in for
one.

**A plain dict for fleet state.** `app/state.py::FleetState` holds eight records in the
backend process. REST handlers and the WebSocket hub read *the same object* on the same
event loop, so consistency is structural — there are not two copies to reconcile. This is
the right shape for one replica and stops being right at more than one; where it moves and
why is in `SYSTEM_DESIGN.md`.

**`seq`, and where at-least-once becomes effectively-once.** Publishers stamp every
message with a strictly increasing token (`sim/robot.py::next_seq`).
`FleetState.apply()` drops anything at or below the highest `seq` already seen for that
robot and returns `None`, so a duplicate never reaches the hub, the history writer, or the
version counter. Without it, a redelivered older reading would move a robot *backwards* on
the operator's screen, to a position it had already left.

**Coalescing fanout, not queues.** Each WebSocket connection holds
`pending: dict[robot_id, RobotState]` rather than a queue (`app/hub.py`). A second update
for `r3` replaces the pending one. Telemetry is last-write-wins, so this is both the
correct semantic and the reason a stalled client cannot apply backpressure to ingest — its
buffer is bounded by the roster size, not by event count.

**The reaper is load-bearing for consistency, not just alerting.** `app/reaper.py` sweeps
for robots that have gone quiet. Staleness derives from elapsed time, not from an arriving
message — so a polling client computing it per-request would flag a silent robot on its
own, while a WebSocket subscriber heard nothing, because silence produces no message to
push. The two interfaces would diverge with no bug anywhere in the transport, the state, or
the serializer. Committing the transition into shared state with a version bump makes it an
event both interfaces observe.

**Which statuses mean what.** The challenge leaves this open deliberately. The call lives
in one dict, `app/health.py::STATUS_CLASS`:

- `active`, `on_mission`, `charging` → **working**. `on_mission` is executing an assigned
  task; `active` is powered movement without one. Charging is a robot doing exactly what it
  should — flagging it would train operators to ignore the attention list.
- `idle`, `maintenance` → **idle**. Planned downtime is already known, so it is not an alarm.
- `blocked`, `error`, `offline`, `unknown` → **attention**.
- Overridden to **attention** by: battery under 20%, staleness, or being unreachable.

## Fault injection

The recorded log is perfectly clean — 181 events per robot, exactly 5s apart, no gaps, no
reordering. That means without injected faults the reconnect and staleness paths would
never execute and could not be demonstrated. Three knobs, off by default:

```bash
CHAOS_DISCONNECT_PROB=0.01 CHAOS_DROP_PROB=0.05 CHAOS_JITTER_MS=800 docker compose up
```

`CHAOS_DISCONNECT_PROB` exits the robot process hard, without a clean MQTT disconnect — so
the broker fires its Last Will, the supervisor restarts it, and it reconnects and resumes.
That is the whole drop-and-recover cycle end to end, not a simulation of one.

## Tests

```bash
python -m venv .venv && .venv/bin/pip install -r backend/requirements.txt -r sim/requirements.txt
.venv/bin/pip install pytest pytest-asyncio httpx
.venv/bin/python -m pytest -q
```

90 tests, no broker or container required. The concentration reflects where the risk is:

- `backend/tests/test_fleet_state.py` — the `seq` guard, including the realistic
  `40, 41, 42, 41` redelivery that would otherwise move a robot backwards.
- `backend/tests/test_api_consistency.py` — the WebSocket snapshot is asserted identical to
  `GET /robots`, and the reaper transition is asserted to be *pushed*, not merely visible
  to a poller. That second one is the regression test for the subtlest failure here:
  asserting only that REST changed would pass even in a world where the two interfaces had
  silently drifted apart.
- `backend/tests/test_publisher_contract.py` — real recorded events through the real
  publisher serializer, across a bytes boundary, into the real ingest handler. The two
  programs never import each other, so nothing else would catch a renamed field; it would
  surface as an empty dashboard at `docker compose up`.
- `backend/tests/test_hub_coalescing.py` — 50 updates to a client that never drains leave
  exactly one pending entry, holding the latest position.
- `backend/tests/test_history.py` — a late reading is refused by state and kept by history,
  in the same call. That separation is what stops a reconnect backlog from being discarded.
- `sim/tests/test_seq.py` — every failure mode of the ordering token is silent, so it gets
  its own file.


## What I cut, and why

- **Authentication.** No operator identity anywhere. It is a bounded addition — FastAPI
  dependency on the routes — and it would have bought nothing the brief asked about.
- **A second backend replica.** The in-process state is a deliberate single-replica design;
  `SYSTEM_DESIGN.md` covers exactly where it breaks and what replaces it.
- **History retention.** The SQLite table grows without bound. Fine for a fifteen-minute
  window, wrong for a real deployment.
- **A command channel to robots.** Telemetry flows one way only. The producer/consumer
  split already has a return path; nothing was built on it.
- **Metrics and tracing.** Structured logs only.

Given more time, in order: the WebSocket fanout work described in `SYSTEM_DESIGN.md` Q2
(serialize once per tick, fixed-interval framing), then MQTT 5 shared subscriptions plus
Redis-backed state for horizontal scale, then the command channel.
