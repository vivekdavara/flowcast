"""Review of day 2: the DuckDB views over a run's outputs."""

import json
from pathlib import Path

import duckdb
import pytest

from flowcast.cli import main
from flowcast.views import COLUMNS, connect


def log(sink: Path, batch: int, files: list[Path]) -> None:
    meta = sink / "_spark_metadata"
    meta.mkdir(parents=True, exist_ok=True)
    lines = ["v1", *(json.dumps({"path": f.as_uri(), "size": 1, "isDir": False, "action": "add"}) for f in files)]
    (meta / str(batch)).write_text("\n".join(lines))


def write_raw_event(sink: Path) -> Path:
    """One raw-events file as Spark writes it: partitioned by event_date, so that column is only in the path."""
    part = sink / "event_date=2026-01-01"
    part.mkdir(parents=True)
    file = part / "part-0.parquet"
    columns = {name: kind for name, kind in COLUMNS["raw_events"].items() if name != "event_date"}
    select = ", ".join(f"NULL::{kind} AS {name}" for name, kind in columns.items())
    with duckdb.connect() as con:
        con.execute(f"COPY (SELECT {select}) TO '{file}'")
    return file


def test_raw_events_has_its_columns_whatever_the_runs_directory_is_called(tmp_path: Path) -> None:
    # A run directory named like a setting (day 4 compares trigger intervals): `trigger=5` is not a partition column.
    root = tmp_path / "trigger=5"
    sink = root / "raw_events"
    log(sink, 0, [write_raw_event(sink)])
    con = connect(root)
    try:
        assert [row[0] for row in con.execute("DESCRIBE raw_events").fetchall()] == list(COLUMNS["raw_events"])
    finally:
        con.close()


def test_query_over_a_log_naming_a_missing_file_is_a_failure_not_a_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sink = tmp_path / "minute_aggregates"
    log(sink, 0, [sink / "part-0.parquet"])  # logged, then removed by hand (or a partial copy of the run)
    assert main(["query", "--out", str(tmp_path), "SELECT count(*) FROM minute_aggregates"]) == 1
    assert "flowcast query:" in capsys.readouterr().err
