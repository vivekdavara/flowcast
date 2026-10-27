"""DuckDB views over the job's Parquet outputs: raw_events, minute_aggregates and dead_letters.

DESIGN.md "DuckDB". A Spark file sink writes a batch's Parquet files first and lists them in its `_spark_metadata` log
when the batch commits, so the directory can hold files of a batch that failed (and will be written again when the
batch is retried). The views read the log, never a glob: a query sees exactly the committed batches. The file list is
taken when the view is made, so `connect` again to see later batches.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import unquote, urlparse

import duckdb

from flowcast.layout import Paths, current_backfill
from flowcast.truth import DUCKDB_COLUMNS

KAFKA_COLUMNS = {"topic": "VARCHAR", "partition": "INTEGER", "offset": "BIGINT", "kafka_timestamp": "TIMESTAMPTZ"}
# Each view's columns, for the empty view of a sink with nothing committed yet (read_parquet needs a file).
COLUMNS = {
    "raw_events": {**DUCKDB_COLUMNS, **KAFKA_COLUMNS, "event_date": "DATE"},
    "minute_aggregates": {
        "window_start": "BIGINT",
        "event_type": "VARCHAR",
        "events": "BIGINT",
        "revenue_cents": "BIGINT",
    },
    "dead_letters": {**KAFKA_COLUMNS, "value": "BLOB", "error": "VARCHAR"},
}
# The batch backfill's result has the stream's aggregate columns.
COLUMNS["backfill_aggregates"] = COLUMNS["minute_aggregates"]


class SinkLogError(Exception):
    pass


def log_files(log: Path) -> tuple[dict[int, Path], dict[int, Path]]:
    """The sink log's batch files and compact files, by batch id."""
    batches: dict[int, Path] = {}
    compacts: dict[int, Path] = {}
    for path in log.iterdir():
        name = path.name
        if name.startswith("."):
            continue  # Hadoop's checksum files and the log's own temporary files
        stem, _, suffix = name.partition(".")
        if not stem.isdigit() or suffix not in ("", "compact"):
            raise SinkLogError(f"{path}: not a sink log file")
        (compacts if suffix else batches)[int(stem)] = path
    return batches, compacts


def latest_batch(sink: Path) -> int:
    """The id of the last batch in a file sink's log (it logs every batch, empty ones too); -1 for none."""
    log = sink / "_spark_metadata"
    if not log.is_dir():
        return -1
    batches, compacts = log_files(log)
    return max([*batches, *compacts], default=-1)


def committed_files(sink: Path) -> list[Path]:
    """The data files of every committed batch of a Spark file sink, from its `_spark_metadata` log, sorted.

    The log has one file per batch (`<id>`), and every `compactInterval` batches a `<id>.compact` holding all entries
    up to it, after which Spark may delete the older files: the latest compact file plus the batch files after it is
    the whole log. Each file is `v1` then one JSON entry per line, with the file's URI and action (add or delete).
    """
    log = sink / "_spark_metadata"
    if not log.is_dir():
        return []
    batches, compacts = log_files(log)
    start = max(compacts, default=-1)
    after = sorted(i for i in batches if i > start)
    if after != list(range(start + 1, start + 1 + len(after))):
        raise SinkLogError(f"{log}: the batch files after {start} aren't consecutive")
    files = ([compacts[start]] if start >= 0 else []) + [batches[i] for i in after]
    live: set[Path] = set()
    for path in files:
        for action, file in log_entries(path):
            if action == "add":
                live.add(file)
            else:
                live.discard(file)
    return sorted(live)


def log_entries(path: Path) -> list[tuple[str, Path]]:
    """One sink log file's entries: ("add" or "delete", the data file). The file is `v1` then a JSON entry a line."""
    lines = path.read_text().splitlines()
    if not lines or lines[0] != "v1":
        raise SinkLogError(f"{path}: not a version-1 sink log file")
    entries = []
    for number, line in enumerate(lines[1:], start=2):
        try:
            entry = json.loads(line)
            uri = urlparse(entry["path"])
            action = entry["action"]
        except (json.JSONDecodeError, KeyError, TypeError):
            raise SinkLogError(f"{path}: line {number} isn't a sink log entry") from None
        if uri.scheme not in ("", "file"):
            raise SinkLogError(f"{path}: {entry['path']} isn't a local file")
        if action not in ("add", "delete"):
            raise SinkLogError(f"{path}: unknown action {action!r}")
        entries.append((action, Path(unquote(uri.path))))
    return entries


def data_files(sink: Path) -> list[Path]:
    """Every Parquet file under a sink's directory, committed or not (the log and hidden files aside), sorted."""
    if not sink.is_dir():
        return []
    return sorted(
        path
        for path in sink.rglob("*.parquet")
        if not any(part.startswith(("_", ".")) for part in path.relative_to(sink).parts)
    )


def orphan_files(sink: Path) -> list[Path]:
    """The sink's data files that no committed batch lists: what a batch wrote before it failed (a crash, a kill)
    and never logged. The retried batch writes new files under new names, so these stay; the views never read them,
    and a reader that globbed the directory would count their rows twice."""
    committed = {path.resolve() for path in committed_files(sink)}
    return [path for path in data_files(sink) if path.resolve() not in committed]


def _view_select(name: str, files: list[Path], *, date_partition: str | None = None) -> str:
    """The query over `files`. Spark keeps a partition column (`date_partition`) only in the directory name, so it is
    read from each file's own parent directory: DuckDB's hive_partitioning would take every key=value segment of
    the whole absolute path, and a run under ".../trigger=5/" would gain a `trigger` column (day-2 review)."""
    if not files:
        columns = ", ".join(f"NULL::{kind} AS {column}" for column, kind in COLUMNS[name].items())
        return f"SELECT {columns} WHERE false"
    listed = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
    # union_by_name: files written before schema version 2 have no campaign column; matched by name, their rows read
    # it as null (by position, DuckDB would refuse the mismatch, or worse, pair up columns that merely line up).
    if date_partition is None:
        return f"SELECT * FROM read_parquet([{listed}], union_by_name = true)"
    date = f"CAST(regexp_extract(filename, '/{date_partition}=([^/]+)/[^/]+$', 1) AS DATE) AS {date_partition}"
    source = f"read_parquet([{listed}], hive_partitioning = false, filename = true, union_by_name = true)"
    return f"SELECT * EXCLUDE (filename), {date} FROM {source}"


def _create_view(
    con: duckdb.DuckDBPyConnection, name: str, files: list[Path], *, date_partition: str | None = None
) -> None:
    """The view over `files`, with every column of COLUMNS[name]. union_by_name gives a column only if some file has
    it, so a run whose files all predate schema version 2 has no campaign column at all, and every query naming it
    (the quality rules do) failed to bind; such a column is added as NULL, as it would read beside newer files."""
    select = _view_select(name, files, date_partition=date_partition)
    present = {row[0] for row in con.execute(f"DESCRIBE {select}").fetchall()}
    missing = [f"NULL::{kind} AS {column}" for column, kind in COLUMNS[name].items() if column not in present]
    if missing:
        select = f"SELECT *, {', '.join(missing)} FROM ({select})"
    con.execute(f"CREATE OR REPLACE VIEW {name} AS {select}")


def connect(root: Path, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the three views over the job's outputs under `root` (layout.Paths), and a fourth,
    `backfill_aggregates`, over the published backfill (empty if there is none)."""
    paths = Paths(root)
    con = duckdb.connect(database)
    try:
        _create_view(con, "raw_events", committed_files(paths.raw_events), date_partition="event_date")
        _create_view(con, "minute_aggregates", committed_files(paths.minute_aggregates))
        _create_view(con, "dead_letters", committed_files(paths.dead_letters))
        # A backfill is published whole (its directory named by the CURRENT pointer), so every file in it counts.
        backfill = current_backfill(paths)
        _create_view(con, "backfill_aggregates", sorted(backfill.glob("*.parquet")) if backfill else [])
    except BaseException:
        con.close()
        raise
    return con
