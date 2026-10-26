"""How fast a run's records became queryable, and how fast the job read them (DESIGN.md "Measuring (day 4)").

A reader sees a file sink's batch when the batch's entry appears in the sink's `_spark_metadata` log (views.py): that
file's modification time is the moment its rows became queryable. Each row then carries two earlier moments:

- `kafka_timestamp`, when the producer created the record (Kafka's CreateTime): produced to queryable is the
  pipeline's own latency, the trigger interval, the batch's work and the commit.
- `event_time`, when the event happened: event to queryable is what a user of the data waits. It adds the network
  delay and lateness the generator gives events, and it is only meaningful when the producer ran in real time with
  event times anchored at the wall clock (`flowcast produce --speed 1 --start-ms now`).

A minute window is queryable when the batch that writes it commits. It can't be written before the watermark passes
its end, so window end to queryable is at least the watermark delay (3 minutes) plus the time until the next batch.

Throughput comes from Spark's progress reports, saved when a run ends (`Queries.save_progress`).
"""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from flowcast.layout import Paths
from flowcast.views import SinkLogError, log_entries, log_files

WINDOW_MS = 60_000


def percentile(sorted_values: Sequence[float], p: float) -> float:
    """The nearest-rank percentile of sorted values: the smallest value with at least p% of them at or below it."""
    if not sorted_values:
        raise ValueError("no values")
    if not 0 < p <= 100:
        raise ValueError("p must be in (0, 100]")
    rank = math.ceil(p / 100 * len(sorted_values))
    return sorted_values[rank - 1]


@dataclass(frozen=True)
class Summary:
    """Percentiles of a set of latencies in milliseconds."""

    count: int
    p50: float
    p95: float
    p99: float
    max: float

    @classmethod
    def of(cls, values: Sequence[float]) -> Summary:
        ordered = sorted(values)
        p50, p95, p99 = (percentile(ordered, p) for p in (50, 95, 99))
        return cls(len(ordered), p50, p95, p99, ordered[-1])

    def line(self, name: str) -> str:
        return (
            f"{name}: {self.count:,} rows, p50 {self.p50:,.0f} ms, p95 {self.p95:,.0f} ms, p99 {self.p99:,.0f} ms, "
            f"max {self.max:,.0f} ms"
        )


@dataclass(frozen=True)
class Commit:
    """One batch of a file sink: when its log entry was written and the data files it added."""

    batch: int
    committed_ms: float
    files: tuple[Path, ...]


def commits(sink: Path) -> list[Commit]:
    """Every batch of the sink in order, from its log. A compact file (every 10th batch) lists all live files so
    far, so its batch's own files are those not live before it. Spark deletes batch files older than a compact
    after a while (cleanupDelay, 10 minutes by default); without them a batch's own files are unknown, so a log
    that doesn't start at batch 0 is refused."""
    log = sink / "_spark_metadata"
    if not log.is_dir():
        return []
    batches, compacts = log_files(log)
    ids = sorted({*batches, *compacts})
    if ids and ids != list(range(len(ids))):
        raise SinkLogError(f"{log}: batches before {ids[0]} or between them were cleaned up; measure a shorter run")
    live: set[Path] = set()
    out = []
    for batch in ids:
        path = compacts.get(batch) or batches[batch]
        entries = log_entries(path)
        if batch in compacts:
            listed = {file for action, file in entries if action == "add"}
            added = listed - live
            live = listed
        else:
            added = {file for action, file in entries if action == "add"}
            live |= added
            live -= {file for action, file in entries if action == "delete"}
        out.append(Commit(batch, path.stat().st_mtime_ns / 1e6, tuple(sorted(added))))
    return out


def _read(con: duckdb.DuckDBPyConnection, commit: Commit, columns: str) -> list[tuple[float, ...]]:
    if not commit.files:
        return []
    listed = ", ".join("'" + str(f).replace("'", "''") + "'" for f in commit.files)
    return con.execute(f"SELECT {columns} FROM read_parquet([{listed}], hive_partitioning = false)").fetchall()


@dataclass(frozen=True)
class Latencies:
    produced: Summary | None  # kafka_timestamp to queryable, per raw event
    event: Summary | None  # event_time to queryable, per raw event
    window: Summary | None  # window end to queryable, per aggregate row
    batches: int  # of the raw-events sink

    def lines(self) -> list[str]:
        named = (
            ("produced to queryable (raw_events)", self.produced),
            ("event time to queryable (raw_events)", self.event),
            ("window end to queryable (minute_aggregates)", self.window),
        )
        return [f"{self.batches} raw_events batches"] + [s.line(n) if s else f"{n}: no rows" for n, s in named]


def latencies(root: Path) -> Latencies:
    """The latencies of the run under `root`, from its two Parquet sinks' logs and files."""
    paths = Paths(root)
    produced: list[float] = []
    event: list[float] = []
    window: list[float] = []
    con = duckdb.connect()
    try:
        raw = commits(paths.raw_events)
        for commit in raw:
            for kafka_ms, event_ms in _read(con, commit, "epoch_ms(kafka_timestamp), event_time"):
                produced.append(commit.committed_ms - kafka_ms)
                event.append(commit.committed_ms - event_ms)
        for commit in commits(paths.minute_aggregates):
            for (start,) in _read(con, commit, "window_start"):
                window.append(commit.committed_ms - (start + WINDOW_MS))
    finally:
        con.close()

    def summary(values: list[float]) -> Summary | None:
        return Summary.of(values) if values else None

    return Latencies(summary(produced), summary(event), summary(window), len(raw))


@dataclass(frozen=True)
class Throughput:
    """One query's reading rate, from its progress reports."""

    query: str
    batches: int
    rows: int
    busy_ms: int  # the batches' own time (triggerExecution), summed
    span_ms: float  # from the first batch's start to the last one's end

    @property
    def rows_per_busy_s(self) -> float:
        return self.rows / self.busy_ms * 1000 if self.busy_ms else 0.0

    @property
    def rows_per_s(self) -> float:
        return self.rows / self.span_ms * 1000 if self.span_ms else 0.0

    def line(self) -> str:
        return (
            f"{self.query}: {self.rows:,} rows in {self.batches} batches over {self.span_ms / 1000:,.1f} s: "
            f"{self.rows_per_s:,.0f} rows/s ({self.rows_per_busy_s:,.0f} per busy second)"
        )


def _epoch_ms(timestamp: str) -> float:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp() * 1000


def throughput(root: Path, query: str) -> Throughput | None:
    """`query`'s rows, batches and time from its saved progress reports; None if there are none. Batches that read
    nothing (a processing-time trigger with no new records still reports some) count toward neither rows nor time."""
    path = Paths(root).progress(query)
    if not path.exists():
        return None
    reports = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    reports = [r for r in reports if r["numInputRows"] > 0]
    if not reports:
        return None
    starts = [_epoch_ms(r["timestamp"]) for r in reports]
    ends = [s + r["durationMs"]["triggerExecution"] for s, r in zip(starts, reports, strict=True)]
    return Throughput(
        query,
        len(reports),
        sum(r["numInputRows"] for r in reports),
        sum(r["durationMs"]["triggerExecution"] for r in reports),
        max(ends) - min(starts),
    )
