"""Fanout behaviour: coalescing, backpressure isolation, and the connect race.

The slow-client case is the one worth demonstrating -- it is the difference between a
stalled dashboard degrading gracefully and a stalled dashboard taking ingest down with it.
"""

from conftest import telemetry

from app.hub import Hub


def test_connect_registers_and_returns_a_snapshot(fleet):
    fleet.apply(telemetry(seq=1))
    conn, snapshot = Hub(fleet).connect()

    assert snapshot["version"] == fleet.version
    assert len(snapshot["robots"]) == 2
    assert conn.pending == {}  # the snapshot covers everything so far; nothing outstanding


def test_publish_reaches_every_subscriber(fleet):
    hub = Hub(fleet)
    first, _ = hub.connect()
    second, _ = hub.connect()

    hub.publish([fleet.apply(telemetry(seq=1))])

    assert set(first.pending) == {"r1"}
    assert set(second.pending) == {"r1"}


async def test_slow_client_gets_latest_only_never_a_backlog(fleet):
    hub = Hub(fleet)
    conn, _ = hub.connect()

    # Fifty updates for one robot while the client never drains.
    for seq in range(1, 51):
        hub.publish([fleet.apply(telemetry(seq=seq, x=float(seq)))])

    # One robot, one pending entry -- not fifty queued frames.
    assert len(conn.pending) == 1

    batch = await conn.drain()
    assert len(batch) == 1
    assert batch[0].x == 50.0  # the current position, not the one it left 49 updates ago
    assert conn.pending == {}


async def test_pending_is_bounded_by_fleet_size_not_by_event_count(fleet):
    hub = Hub(fleet)
    conn, _ = hub.connect()

    for seq in range(1, 101):
        for robot_id in ("r1", "r2"):
            hub.publish([fleet.apply(telemetry(robot_id=robot_id, seq=seq))])

    # 200 events, 2 robots. The buffer cannot grow past the roster.
    assert len(conn.pending) == 2


async def test_drain_returns_only_what_changed_since_last_drain(fleet):
    hub = Hub(fleet)
    conn, _ = hub.connect()

    hub.publish([fleet.apply(telemetry(robot_id="r1", seq=1))])
    assert [r.robot_id for r in await conn.drain()] == ["r1"]

    hub.publish([fleet.apply(telemetry(robot_id="r2", seq=1))])
    assert [r.robot_id for r in await conn.drain()] == ["r2"]


async def test_updates_during_connect_arrive_exactly_once(fleet):
    """The join race: an update must not be lost in the handshake, nor sent twice.

    Hub.connect() registers and snapshots with no await between them, so on a single
    event loop the pair is indivisible. Anything before connect() is in the snapshot;
    anything after is a delta; nothing lands in both or neither.
    """
    hub = Hub(fleet)

    hub.publish([fleet.apply(telemetry(robot_id="r1", seq=1, x=1.0))])  # before

    conn, snapshot = hub.connect()

    hub.publish([fleet.apply(telemetry(robot_id="r1", seq=2, x=2.0))])  # after

    snapshot_r1 = [r for r in snapshot["robots"] if r["robot_id"] == "r1"][0]
    assert snapshot_r1["x"] == 1.0  # the snapshot froze at connect time

    batch = await conn.drain()
    assert len(batch) == 1  # exactly one delta, not zero and not a repeat of the snapshot
    assert batch[0].x == 2.0


def test_disconnect_stops_delivery(fleet):
    hub = Hub(fleet)
    conn, _ = hub.connect()
    hub.disconnect(conn)

    hub.publish([fleet.apply(telemetry(seq=1))])

    assert conn.pending == {}
    assert hub.subscriber_count == 0


async def test_close_wakes_a_waiting_drain(fleet):
    # Without this the sender coroutine would hang on drain() forever after the socket
    # went away, leaking a task per dropped client.
    conn, _ = Hub(fleet).connect()
    conn.close()

    assert await conn.drain() == []


def test_publishing_nothing_is_a_no_op(fleet):
    hub = Hub(fleet)
    conn, _ = hub.connect()

    hub.publish([])  # what apply() returning None for every message looks like

    assert conn.pending == {}
