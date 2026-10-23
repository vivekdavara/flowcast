"""Ground truth: the exact minute aggregates of a set of events, in Python and (the same definition) in DuckDB SQL.

DESIGN.md "Ground truth". The generator made every event, so these are the answers the streaming job is checked
against. Duplicates (the same event_id) count once, as the first copy in input order: the generator's copies are the
same bytes, but a file passed to `flowcast truth --input` may hold copies that differ, and both engines must agree.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

import duckdb

from flowcast.schema import Event, encode

WINDOW_MS = 60_000

# DuckDB column types for the version-1 fields, so read_json never guesses (a column of nulls would become JSON).
DUCKDB_COLUMNS = {
    "schema_version": "INTEGER",
    "event_id": "VARCHAR",
    "event_type": "VARCHAR",
    "event_time": "BIGINT",
    "user_id": "VARCHAR",
    "session_id": "VARCHAR",
    "device": "VARCHAR",
    "country": "VARCHAR",
    "page": "VARCHAR",
    "product_id": "VARCHAR",
    "category": "VARCHAR",
    "price_cents": "BIGINT",
    "quantity": "INTEGER",
}


@dataclass(frozen=True, slots=True, order=True)
class Row:
    """One window and event type: [window_start, window_start + 60 s)."""

    window_start: int
    event_type: str
    events: int
    revenue_cents: int


def window_start(event_time: int, window_ms: int = WINDOW_MS) -> int:
    """The start of the tumbling window holding `event_time` (Spark's window() alignment, from the epoch)."""
    return event_time // window_ms * window_ms


def minute_aggregates(events: Iterable[Event]) -> list[Row]:
    """The truth in Python, sorted by window then event type."""
    seen: set[str] = set()
    totals: dict[tuple[int, str], tuple[int, int]] = {}
    for event in events:
        if event.event_id in seen:
            continue
        seen.add(event.event_id)
        key = (window_start(event.event_time), event.event_type)
        count, revenue = totals.get(key, (0, 0))
        totals[key] = (count + 1, revenue + event.revenue_cents())
    return [Row(start, event_type, count, revenue) for (start, event_type), (count, revenue) in sorted(totals.items())]


def minute_aggregates_sql() -> str:
    return resources.files("flowcast").joinpath("sql/minute_aggregates.sql").read_text()


def duckdb_minute_aggregates(events: Iterable[Event]) -> list[Row]:
    """The truth in DuckDB: the events written as JSON lines (their wire form), read back by read_json, aggregated by
    sql/minute_aggregates.sql."""
    with tempfile.TemporaryDirectory(prefix="flowcast-") as tmp:
        path = Path(tmp) / "events.jsonl"
        with path.open("wb") as out:
            for event in events:
                out.write(encode(event) + b"\n")
        con = duckdb.connect()
        try:
            # Insertion keeps the file's order (preserve_insertion_order, set here because the next step relies on
            # it), so a row's rowid is its line number: the input order the SQL keeps the first copy by.
            con.execute("SET preserve_insertion_order = true")
            con.execute(
                "CREATE TABLE wire AS SELECT * FROM read_json(?, format = 'newline_delimited', columns = ?)",
                [str(path), DUCKDB_COLUMNS],
            )
            con.execute("CREATE TABLE events AS SELECT rowid AS input_order, * FROM wire")
            rows = con.execute(minute_aggregates_sql()).fetchall()
        finally:
            con.close()
    return [Row(int(start), str(event_type), int(count), int(revenue)) for start, event_type, count, revenue in rows]
