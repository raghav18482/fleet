"""WebSocket fanout.

Each connection holds a *coalescing* buffer keyed by robot_id rather than a queue. A
second update for r3 replaces the pending one instead of lining up behind it.

Two reasons, both load-bearing:

  Correct semantics. Telemetry is last-write-wins. Nobody operating a fleet wants to
  watch a forty-frame backlog of positions a robot has already left; they want where it
  is now.

  No backpressure. A slow or stalled client cannot grow an unbounded queue and cannot
  block ingest, because its buffer is capped at one entry per robot -- eight, here. It
  sees *fewer* frames, never staler ones.
"""

import asyncio


class Connection:
    """One subscriber's pending view. Not created directly -- see Hub.connect()."""

    def __init__(self):
        self.pending = {}
        self._wakeup = asyncio.Event()
        self.closed = False

    def offer(self, robot):
        """Buffer a robot's latest state, replacing any earlier pending entry for it."""
        self.pending[robot.robot_id] = robot
        self._wakeup.set()

    async def drain(self):
        """Wait for changes, then take everything pending. Empty list means closed."""
        await self._wakeup.wait()
        self._wakeup.clear()
        if self.closed:
            return []
        batch = list(self.pending.values())
        self.pending.clear()
        return batch

    def close(self):
        self.closed = True
        self._wakeup.set()  # wake a waiting drain() so the sender loop can exit


class Hub:
    def __init__(self, state):
        self._state = state
        self._connections = set()

    def connect(self):
        """Register a subscriber and take its opening snapshot, atomically.

        There is deliberately no `await` between registering the connection and reading
        the snapshot. On a single-threaded event loop that makes the pair indivisible,
        which closes the join race at both ends: an update landing mid-handshake cannot
        slip through the gap and be missed, and cannot be delivered twice (once inside
        the snapshot, once as a delta).

        Registration comes first so that the failure mode, if this ever did get split,
        is a duplicate rather than a loss -- a client can reconcile a repeat by version,
        but it cannot recover something it never received.
        """
        conn = Connection()
        self._connections.add(conn)
        return conn, self._state.snapshot()

    def disconnect(self, conn):
        conn.close()
        self._connections.discard(conn)

    def publish(self, robots):
        """Offer changed robots to every subscriber. Synchronous and non-blocking."""
        if not robots:
            return
        for conn in self._connections:
            for robot in robots:
                conn.offer(robot)

    @property
    def subscriber_count(self):
        return len(self._connections)
