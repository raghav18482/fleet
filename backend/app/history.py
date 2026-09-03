"""Append-only telemetry history, for GET /robots/history/{robot_id}.

SQLite because it needs no extra container, no extra service in compose, and no
operational story beyond a file — for a single-writer append log of a few rows per second
it is exactly the right size of tool, and it keeps `docker compose up` at three services.

Writes are batched on a timer rather than committed per message. SQLite's driver is
synchronous, so committing inside the ingest path would block the event loop on every
reading; accumulating for a second and issuing one executemany keeps disk I/O off the
path that WebSocket latency depends on.
"""

import logging
import sqlite3

import asyncio

from .config import DB_PATH, HISTORY_FLUSH_S

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry (
    robot_id TEXT NOT NULL,
    seq      INTEGER NOT NULL,
    ts       REAL NOT NULL,
    x        REAL NOT NULL,
    y        REAL NOT NULL,
    status   TEXT NOT NULL,
    battery  REAL NOT NULL,
    PRIMARY KEY (robot_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_telemetry_robot_ts ON telemetry (robot_id, ts);
"""


class HistoryWriter:
    def __init__(self, db_path=DB_PATH, flush_interval=HISTORY_FLUSH_S):
        self._db_path = db_path
        self._flush_interval = flush_interval
        self._buffer = []
        self._conn = None

    def open(self):
        self._conn = sqlite3.connect(self._db_path)
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        return self

    def close(self):
        if self._conn is not None:
            self.flush()
            self._conn.close()
            self._conn = None

    def record(self, robot):
        """Buffer one reading. Cheap and synchronous — safe to call from ingest."""
        if robot.seq < 0:
            return  # seeded state, never actually reported
        self._buffer.append(
            (robot.robot_id, robot.seq, robot.ts, robot.x, robot.y, robot.status, robot.battery)
        )

    def flush(self):
        if not self._buffer or self._conn is None:
            return 0
        batch, self._buffer = self._buffer, []
        # A redelivery the seq guard already rejected never reaches here, but INSERT OR
        # IGNORE makes the primary key the backstop rather than a crash.
        self._conn.executemany(
            "INSERT OR IGNORE INTO telemetry "
            "(robot_id, seq, ts, x, y, status, battery) VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        self._conn.commit()
        return len(batch)

    def query(self, robot_id, t_from=None, t_to=None, limit=1000):
        sql = "SELECT robot_id, seq, ts, x, y, status, battery FROM telemetry WHERE robot_id = ?"
        args = [robot_id]
        if t_from is not None:
            sql += " AND ts >= ?"
            args.append(t_from)
        if t_to is not None:
            sql += " AND ts <= ?"
            args.append(t_to)
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)

        rows = self._conn.execute(sql, args).fetchall()
        return [
            {
                "robot_id": r[0],
                "seq": r[1],
                "ts": r[2],
                "x": r[3],
                "y": r[4],
                "status": r[5],
                "battery": r[6],
            }
            for r in rows
        ]

    async def run(self):
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                self.flush()
            except Exception:
                log.exception("history flush failed")
