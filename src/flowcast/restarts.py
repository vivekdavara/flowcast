"""Where each query stood when the job started: the facts a restart leaves no other trace of.

A query's last batch is planned (`offsets/<id>`), then written and logged by its sink, then committed
(`commits/<id>`). A crash can stop it between any two of these, and on restart Spark runs an uncommitted batch again.
If the file sink had already logged it, the sink skips it **without executing the batch** (Spark's FileStreamSink
returns early for a batch id in its log), so the batch's event times never reach the watermark tracker: the next
watermark is the re-run batch's own, not the maximum event time of its records minus the delay. Found on day 3 by the
kill test (DESIGN.md "Exactly once"). The output is still exact; only the watermark lags until a later batch's events
pass the skipped ones'.

The checkpoint doesn't record that a batch was skipped, so the job does: `start` appends each query's state to
`<root>/_restarts.jsonl` before it starts the queries, and the check reads it to know which batches' records the
watermark never saw. A `foreachBatch` sink (the DuckDB one) is always called, and the frame it collects is executed,
so its batches are always seen.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

from flowcast.duckdb_sink import read
from flowcast.layout import Paths
from flowcast.views import latest_batch

DUCKDB_QUERY = "duckdb_aggregates"
# The queries whose sink is Spark's file sink, which skips a logged batch on a re-run without executing it.
FILE_SINK_QUERIES = ("dead_letters", "raw_events", "minute_aggregates")


def _last(directory: Path) -> int:
    if not directory.is_dir():
        return -1
    return max((int(p.name) for p in directory.iterdir() if p.name.isdigit()), default=-1)


@dataclass(frozen=True)
class QueryState:
    """Where a query's last batch had got to: planned (offset log), logged (sink log), committed (commit log)."""

    planned: int
    logged: int
    committed: int

    @property
    def phase(self) -> str:
        if self.planned == self.committed:
            return "between batches"
        if self.logged > self.committed:
            return "logged, not committed"  # the sink's log is ahead: on restart the sink must skip the batch
        return "planned, not logged"  # mid-batch: its files (if any) become orphans, and the batch runs again


def query_state(paths: Paths, name: str) -> QueryState:
    checkpoint = paths.checkpoint(name)
    if name == DUCKDB_QUERY:
        logged = read(paths.duckdb)[1]  # the last batch whose transaction committed
    else:
        sink = getattr(paths, name)
        assert isinstance(sink, Path)
        logged = latest_batch(sink)
    return QueryState(_last(checkpoint / "offsets"), logged, _last(checkpoint / "commits"))


def restarts_file(paths: Paths) -> Path:
    return paths.root / "_restarts.jsonl"


def record_start(paths: Paths, names: Iterable[str]) -> dict[str, QueryState]:
    """Append the queries' states to the run's restart log, as one line, before the queries start."""
    states = {name: query_state(paths, name) for name in names}
    line = json.dumps({"started_at": time.time(), "queries": {n: asdict(s) for n, s in states.items()}}) + "\n"
    path = restarts_file(paths)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        # A start killed mid-write leaves a torn last line (and started nothing): end it, so this line stands alone.
        if os.fstat(fd).st_size and not path.read_bytes().endswith(b"\n"):
            line = "\n" + line
        os.write(fd, line.encode())
    finally:
        os.close(fd)
    return states


def read_starts(paths: Paths) -> list[dict[str, QueryState]]:
    """Every recorded start of the job, in order. A line that isn't whole JSON is a start killed while writing it,
    before any query began, and is skipped."""
    path = restarts_file(paths)
    if not path.exists():
        return []
    starts = []
    for line in path.read_text().splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        starts.append({name: QueryState(**state) for name, state in entry["queries"].items()})
    return starts


def skipped_reruns(starts: Iterable[dict[str, QueryState]], name: str) -> set[int]:
    """The batches of a file-sink query that a restart ran again with the batch already in the sink's log: skipped
    by the sink, so their records never moved the watermark."""
    if name not in FILE_SINK_QUERIES:
        return set()
    return {start[name].planned for start in starts if name in start and start[name].phase == "logged, not committed"}
