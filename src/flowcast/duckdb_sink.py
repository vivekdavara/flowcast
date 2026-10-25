"""An idempotent DuckDB sink for the minute aggregates (DESIGN.md "Exactly once").

Spark's `foreachBatch` hands each micro-batch to Python with its batch id. After a crash, Spark runs the batch it
hadn't committed again, with the same id and the same offsets, and the sink is called a second time. So a batch's rows
and its id go into the database in one transaction, and an id already there is skipped: the second call changes
nothing, and a crash inside the transaction leaves neither the rows nor the id.

Batch ids only identify a batch within one query: a new checkpoint starts again at 0. So the database also records the
query's id (Spark's `<checkpoint>/metadata`, the same across restarts from one checkpoint) and refuses batches from
any other query, which it would otherwise skip as already written. Batch ids must also arrive in order with no gap
(Spark calls the sink for every batch, empty ones too): a gap means the database isn't the one this checkpoint wrote.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb

from flowcast.truth import Row

_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS sink_batches (
        query_id VARCHAR NOT NULL,
        batch_id BIGINT NOT NULL PRIMARY KEY,
        rows BIGINT NOT NULL,
        written_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    # Append mode writes a window once, so a second row for a window and type is a bug, not an update: the key
    # refuses it.
    """CREATE TABLE IF NOT EXISTS minute_aggregates (
        window_start BIGINT NOT NULL,
        event_type VARCHAR NOT NULL,
        events BIGINT NOT NULL,
        revenue_cents BIGINT NOT NULL,
        batch_id BIGINT NOT NULL,
        PRIMARY KEY (window_start, event_type)
    )""",
)


class SinkError(Exception):
    pass


def query_id(checkpoint: Path) -> str:
    """The streaming query's id from its checkpoint: Spark writes it when the query first starts and keeps it."""
    return str(json.loads((checkpoint / "metadata").read_text())["id"])


@dataclass(frozen=True)
class DuckDBSink:
    database: Path
    checkpoint: Path  # the query's checkpoint, whose id the database is bound to

    def write(self, batch_id: int, rows: Sequence[Row]) -> bool:
        """Write one batch's rows and its id in one transaction. False (and nothing written) if the batch is already
        there: Spark running it again after a crash."""
        owner = query_id(self.checkpoint)
        con = duckdb.connect(str(self.database))
        try:
            for statement in _SCHEMA:
                con.execute(statement)
            # Closing the connection without COMMIT (on a refusal, a skip or an error) rolls the transaction back.
            con.execute("BEGIN TRANSACTION")
            others = con.execute("SELECT DISTINCT query_id FROM sink_batches WHERE query_id <> ?", [owner]).fetchone()
            if others is not None:
                raise SinkError(f"{self.database} holds batches of query {others[0]}, not {owner}")
            last = con.execute("SELECT coalesce(max(batch_id), -1) FROM sink_batches").fetchone()
            assert last is not None
            if batch_id <= last[0]:
                return False
            if batch_id != last[0] + 1:
                raise SinkError(f"batch {batch_id} after batch {last[0]}: {self.database} is missing batches")
            if rows:  # executemany refuses an empty list, and most batches of a stream write no window
                con.executemany(
                    "INSERT INTO minute_aggregates VALUES (?, ?, ?, ?, ?)",
                    [(r.window_start, r.event_type, r.events, r.revenue_cents, batch_id) for r in rows],
                )
            con.execute(
                "INSERT INTO sink_batches (query_id, batch_id, rows) VALUES (?, ?, ?)", [owner, batch_id, len(rows)]
            )
            con.execute("COMMIT")
            return True
        finally:
            con.close()


def read(database: Path) -> tuple[list[Row], int]:
    """The rows in a sink's database, sorted, and its last batch id (-1 for none). Opened read-only."""
    if not database.exists():
        return [], -1
    con = duckdb.connect(str(database), read_only=True)
    try:
        rows = con.execute(
            "SELECT window_start, event_type, events, revenue_cents FROM minute_aggregates ORDER BY ALL"
        ).fetchall()
        last = con.execute("SELECT coalesce(max(batch_id), -1) FROM sink_batches").fetchone()
    finally:
        con.close()
    assert last is not None
    return [Row(int(a), str(b), int(c), int(d)) for a, b, c, d in rows], int(last[0])
