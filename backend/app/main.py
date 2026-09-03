"""FastAPI application: the two interfaces onto FleetState.

`GET /robots` and `WS /ws/fleet` are the same data through different doors. They read the
same FleetState instance on the same event loop and serialize through the same function,
so there is no reconciliation step between them — there is nothing to reconcile.

The guarantee, stated precisely: a poller and a subscriber never observe *contradictory*
state. A subscriber may lag by up to one drain, but it converges — it never sees a value
that was never committed, and never moves backwards. Every payload from both carries the
same `version`, so a client can prove which view it holds.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

from .config import load_roster
from .history import HistoryWriter
from .hub import Hub
from .ingest import run_ingest
from .models import serialize_robot
from .reaper import run_reaper
from .state import FleetState

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def create_app(state=None, hub=None, history=None, start_tasks=True):
    """Build the app.

    Tests inject their own state/hub and pass start_tasks=False, so every route can be
    exercised without a broker anywhere in the picture.
    """
    fleet = state if state is not None else FleetState(load_roster())
    fanout = hub if hub is not None else Hub(fleet)

    @asynccontextmanager
    async def lifespan(app):
        store = history
        tasks = []
        if start_tasks:
            store = store or HistoryWriter().open()
            tasks = [
                asyncio.create_task(run_ingest(fleet, fanout, store), name="ingest"),
                asyncio.create_task(run_reaper(fleet, fanout), name="reaper"),
                asyncio.create_task(store.run(), name="history-flush"),
            ]
        app.state.fleet, app.state.hub, app.state.history = fleet, fanout, store
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if start_tasks and store is not None:
                store.close()

    app = FastAPI(title="Fleet backend", version="1.0.0", lifespan=lifespan)
    app.state.fleet, app.state.hub, app.state.history = fleet, fanout, history

    # Every route below is `async def`, and that is load-bearing rather than stylistic.
    #
    # FastAPI runs a plain `def` handler in a threadpool. A handler reading FleetState
    # from a worker thread while the ingest task mutates it on the event loop can observe
    # a torn read -- `version` from one instant and robot data from another -- which is
    # precisely the inconsistency between the polling and streaming interfaces that this
    # design claims to make impossible. `async def` keeps every read on the same event
    # loop as every write, so a handler runs to completion between awaits and sees one
    # coherent instant.
    #
    # The reads are all in-memory and non-blocking, so nothing is given up by not
    # deferring them to a thread.

    @app.get("/health")
    async def health_check():
        return {"status": "ok", "version": fleet.version, "robots": len(fleet.robots())}

    @app.get("/robots")
    async def list_robots(
        health: str = Query(None, description="working | idle | attention"),
        status: str = Query(None, description="raw reported status, e.g. error"),
    ):
        """Current fleet state. Identical in shape to the WebSocket snapshot frame."""
        snapshot = fleet.snapshot()
        robots = snapshot["robots"]
        if health:
            robots = [r for r in robots if r["health"] == health]
        if status:
            robots = [r for r in robots if r["status"] == status]
        return {"version": snapshot["version"], "robots": robots}

    @app.get("/robots/history/{robot_id}")
    async def robot_history(
        robot_id: str,
        t_from: float = Query(None, alias="from"),
        t_to: float = Query(None, alias="to"),
        limit: int = Query(1000, ge=1, le=10000),
    ):
        if fleet.get(robot_id) is None:
            raise HTTPException(status_code=404, detail="unknown robot " + robot_id)
        store = app.state.history
        if store is None:
            raise HTTPException(status_code=503, detail="history is not enabled")
        # Flush first so a reading that just arrived is queryable rather than sitting in
        # the write buffer looking like data loss.
        store.flush()
        return {"robot_id": robot_id, "points": store.query(robot_id, t_from, t_to, limit)}

    @app.get("/robots/{robot_id}")
    async def get_robot(robot_id: str):
        robot = fleet.get(robot_id)
        if robot is None:
            raise HTTPException(status_code=404, detail="unknown robot " + robot_id)
        return {"version": fleet.version, "robot": serialize_robot(robot, time.time())}

    @app.get("/fleet/summary")
    async def fleet_summary():
        by_status, by_health = {}, {}
        for robot in fleet.robots():
            by_status[robot.status] = by_status.get(robot.status, 0) + 1
            verdict = robot.health().value
            by_health[verdict] = by_health.get(verdict, 0) + 1
        return {
            "version": fleet.version,
            "total": len(fleet.robots()),
            "by_status": by_status,
            "by_health": by_health,
        }

    @app.websocket("/ws/fleet")
    async def stream_fleet(websocket: WebSocket):
        """Snapshot on connect, then coalesced deltas.

        A client that drops and reconnects needs no special handling: it simply gets a
        fresh snapshot, so recovery is the same code path as first connect.
        """
        await websocket.accept()
        connection, snapshot = fanout.connect()
        try:
            await websocket.send_json(dict(type="snapshot", **snapshot))
            while True:
                batch = await connection.drain()
                if connection.closed:
                    break
                now = time.time()
                await websocket.send_json(
                    {
                        "type": "update",
                        "version": fleet.version,
                        "robots": [serialize_robot(r, now) for r in batch],
                    }
                )
        except WebSocketDisconnect:
            pass
        finally:
            fanout.disconnect(connection)

    return app


app = create_app()
