# Answers

## 1. What holds the fleet's current state, and why that shape?

`FleetState` in `backend/app/state.py` — a `dict[str, RobotState]` holding eight records
inside the backend process, seeded from `robots.json` at construction so all eight exist
from boot rather than appearing one at a time as telemetry arrives. A consumer connecting
before the first message sees eight unaccounted-for robots, which is honest; an empty
fleet that fills in gradually is indistinguishable from the fleet having vanished.

The shape is driven directly by the requirement that both interfaces agree. `list_robots`
and `stream_fleet` in `backend/app/main.py` read *the same object* on the same asyncio
event loop, and both serialize through the same function
(`backend/app/models.py::serialize_robot`). There is no synchronisation step between them
because there are not two copies to synchronise — consistency is structural rather than
coordinated. A shared store would have added a network hop and a second source of truth to
keep aligned, making the guarantee weaker, not stronger. `RobotState` is a frozen
dataclass and `apply()` replaces rather than mutates, so a snapshot already handed to a
REST response cannot be altered underneath it by an update that lands a moment later.

The precise guarantee is worth stating, because "they share a dict" oversells it. A
subscriber may **lag** a poller by up to one drain, but it **converges**: every value it
receives was genuinely committed, and it never moves backwards. Lag is not inconsistency.
Every payload from both interfaces carries the same `version` (bumped only on an accepted
mutation), so a client can prove which view it holds.

The non-obvious half is `backend/app/reaper.py`. Staleness derives from elapsed time, not
from an arriving message — so a poller computing it per-request would flag a silent robot
entirely on its own, while a subscriber heard nothing, because silence produces no message
to push. The two views would diverge with no bug anywhere in the transport, the state, or
the serializer. `sweep_stale()` commits the transition into shared state with a version
bump and the reaper fans it out, which turns "went quiet" into an event both interfaces
observe. `test_reaper_transition_is_pushed_not_merely_inferred` is the regression test;
asserting only that REST had changed would have passed even in the broken world.

## 2. The transport, its guarantees, and reconciling them with fanout

MQTT via Mosquitto, publishing to `fleet/robots/{id}/telemetry` at **QoS 1** with
`clean_session=False` (`sim/robot.py::connect`, `backend/app/ingest.py::run_ingest`).

QoS 1 is at-least-once: the broker redelivers anything it did not get an ack for, so
duplicates are expected rather than exceptional, and a delayed message can arrive behind a
newer one. Two further properties were chosen for what they buy: `clean_session=False`
makes the broker queue messages for the backend while it is restarting instead of dropping
them, and each robot's **Last Will** means the broker announces that robot's death on its
behalf when the connection dies — the robot arranges in advance to report a failure it
could never be alive to report.

The reconciliation with fanout is one comparison, in one place. `FleetState.apply()`
returns `None` for any message whose `seq` is at or below the highest already seen for that
robot, so a duplicate never reaches the hub, the history writer, or the version counter:
at-least-once transport, effectively-once fanout. The same comparison handles both failure
modes, since a redelivery and a late straggler are indistinguishable at the receiver.
`test_redelivery_does_not_move_a_robot_backwards` drives the realistic case — arrival order
`40, 41, 42, 41` — and asserts the robot stays at 42's position rather than jumping back to
one it had already left.

**The cost, honestly.** An extra container and broker configuration versus robots POSTing
directly. A `seq` that publishers must keep strictly increasing across process restarts —
`sim/robot.py::next_seq` uses `max(clock_ms, last + 1)` because a plain counter resets to 0
on restart (the guard would then reject everything the robot sent, and it would look
permanently dead) while a bare timestamp collides inside a single millisecond (the guard
would drop the second reading silently). A test caught that second case; inspection had
not. And at-least-once means a reconnect can deliver a queued burst rather than a steady
trickle, so "when did the operator actually see this?" is harder to reason about than a
synchronous push would be. The coalescing hub absorbs the burst, but it absorbs it by
discarding intermediate states — correct for telemetry, and still a real loss of fidelity.

## 3. What I left out, and what I would build next

Left out: authentication of any kind; a second backend replica; history retention or
downsampling, so the SQLite table grows unbounded; a command channel back to the robots;
and metrics or tracing beyond structured logs. Each was a scope decision rather than an
oversight, and `SYSTEM_DESIGN.md` names where each would attach.

I also deliberately did not build fidelity I could not justify. The recorded log is
perfectly clean — 181 events per robot, exactly 5s apart, no gaps or reordering — so the
reconnect and staleness paths would never have executed under a faithful replay. Rather
than assume that code worked, the publishers carry fault injection
(`CHAOS_DISCONNECT_PROB`, `CHAOS_DROP_PROB`, `CHAOS_JITTER_MS`), with the disconnect case
exiting the process hard so the broker genuinely fires the Last Will and the supervisor
genuinely restarts it. What is *not* covered is true out-of-order arrival: a single
sequential publisher cannot produce it, so that path is proven only at unit level in
`test_lower_seq_is_dropped`, not end to end.

Next, in order. First the fanout work in `SYSTEM_DESIGN.md` Q2 — serialize each robot once
per tick instead of once per client, then move to fixed-interval framing — because it is
the only cost that grows with the product of robots and clients rather than either alone.
Then horizontal scale: MQTT 5 shared subscriptions to load-balance ingest across replicas,
with `FleetState` moving to Redis and cross-replica fanout over Redis Pub/Sub. Then the
command channel, which the producer/consumer split already has a natural return path for.
