"""latency.py: nearest-rank percentiles, each batch's own files and commit time, the latencies and throughput."""

import json
import os
from pathlib import Path

import duckdb
import pytest

from flowcast.cli import main
from flowcast.latency import Summary, commits, latencies, percentile, throughput
from flowcast.layout import Paths
from flowcast.views import SinkLogError
from tests.test_views import log_file

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def test_percentiles_are_nearest_rank() -> None:
    hundred = [float(n) for n in range(1, 101)]
    assert [percentile(hundred, p) for p in (1, 50, 95, 99, 100)] == [1, 50, 95, 99, 100]
    assert percentile([7.0], 50) == percentile([7.0], 99) == 7
    assert percentile([1.0, 2.0], 50) == 1  # half of them at or below it
    assert percentile([1.0, 2.0], 51) == 2
    assert percentile([1.0, 2.0, 3.0], 95) == 3
    with pytest.raises(ValueError, match="no values"):
        percentile([], 50)
    with pytest.raises(ValueError, match="p must be"):
        percentile([1.0], 0)


def test_percentile_ranks_are_exact_in_any_float_order() -> None:
    """ceil(p * n / 100) in floats is wrong too, for other p and n than the review's (mutation pass, day 5): 64.4% of
    250 is 161 exactly, and 64.4 * 250 / 100 is 161.00000000000003."""
    values = [float(v) for v in range(1, 1001)]
    assert percentile(values[:250], 64.4) == 161
    assert percentile(values[:375], 8.8) == 33
    assert percentile(values, 16.1) == 161


def test_a_summary_sorts_first() -> None:
    assert Summary.of([30.0, 10.0, 20.0]) == Summary(3, 20, 30, 30, 30)
    assert Summary(3, 20, 30, 30, 30).line("x") == "x: 3 rows, p50 20 ms, p95 30 ms, p99 30 ms, max 30 ms"


def commit_at(sink: Path, name: str, entries: list[tuple[Path, str]], ms: int) -> None:
    log_file(sink, name, entries)
    os.utime(sink / "_spark_metadata" / name, ns=(ms * 1_000_000, ms * 1_000_000))


def test_each_batch_has_its_own_files_and_commit_time(tmp_path: Path) -> None:
    f = [tmp_path / f"part-{n}.parquet" for n in range(5)]
    commit_at(tmp_path, "0", [(f[0], "add")], T0 + 1_000)
    commit_at(tmp_path, "1", [(f[1], "add"), (f[2], "add")], T0 + 2_000)
    commit_at(tmp_path, "2", [], T0 + 3_000)  # a batch that read nothing
    # A compact file lists every live file so far; its batch's own are the new ones.
    commit_at(tmp_path, "3.compact", [(f[0], "add"), (f[1], "add"), (f[2], "add"), (f[3], "add")], T0 + 4_000)
    commit_at(tmp_path, "4", [(f[4], "add")], T0 + 5_000)
    assert [(c.batch, c.committed_ms, c.files) for c in commits(tmp_path)] == [
        (0, T0 + 1_000, (f[0],)),
        (1, T0 + 2_000, (f[1], f[2])),
        (2, T0 + 3_000, ()),
        (3, T0 + 4_000, (f[3],)),
        (4, T0 + 5_000, (f[4],)),
    ]
    assert commits(tmp_path / "missing") == []


def test_a_deleted_file_listed_again_is_the_compact_batchs_own(tmp_path: Path) -> None:
    """A delete takes a file out of the live set, so a compact that lists it again added it in its own batch
    (mutation pass, day 5: without the delete, the file was counted as an old one)."""
    f = [tmp_path / f"part-{n}.parquet" for n in range(2)]
    commit_at(tmp_path, "0", [(f[0], "add"), (f[1], "add")], T0 + 1_000)
    commit_at(tmp_path, "1", [(f[0], "delete")], T0 + 2_000)
    commit_at(tmp_path, "2.compact", [(f[0], "add"), (f[1], "add")], T0 + 3_000)
    assert [(c.batch, c.files) for c in commits(tmp_path)] == [(0, (f[0], f[1])), (1, ()), (2, (f[0],))]


def test_a_log_cleaned_up_before_a_compact_is_refused(tmp_path: Path) -> None:
    commit_at(tmp_path, "9.compact", [], T0)
    commit_at(tmp_path, "10", [], T0)
    with pytest.raises(SinkLogError, match="cleaned up"):
        commits(tmp_path)


def parquet(path: Path, sql: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")


def raw_file(path: Path, rows: list[tuple[int, int]]) -> None:
    """Raw events with (kafka timestamp, event time), both in epoch ms."""
    values = ", ".join(f"(make_timestamptz({k * 1000}::BIGINT), {e}::BIGINT)" for k, e in rows)
    parquet(path, f"SELECT * FROM (VALUES {values}) AS t(kafka_timestamp, event_time)")


def run(tmp_path: Path) -> Paths:
    paths = Paths(tmp_path)
    a = paths.raw_events / "event_date=2026-01-01" / "part-a.parquet"
    b = paths.raw_events / "event_date=2026-01-01" / "part-b.parquet"
    raw_file(a, [(T0 + 100, T0), (T0 + 200, T0 + 50)])
    raw_file(b, [(T0 + 1_100, T0 - 30_000)])  # a late event: produced 31.1 s after it happened
    commit_at(paths.raw_events, "0", [(a, "add")], T0 + 1_000)
    commit_at(paths.raw_events, "1", [(b, "add")], T0 + 2_000)
    w = paths.minute_aggregates / "part-w.parquet"
    parquet(w, f"SELECT * FROM (VALUES ({T0}::BIGINT), ({T0}::BIGINT)) AS t(window_start)")
    commit_at(paths.minute_aggregates, "0", [], T0 + 1_000)
    commit_at(paths.minute_aggregates, "1", [(w, "add")], T0 + 245_000)
    return paths


def test_latencies_from_the_commit_times(tmp_path: Path) -> None:
    result = latencies(run(tmp_path).root)
    assert result.batches == 2
    assert result.produced == Summary.of([900, 800, 900])
    assert result.event == Summary.of([1_000, 950, 32_000])
    assert result.window == Summary.of([185_000, 185_000])  # the window ended at T0 + 60 s
    assert result.lines()[1] == (
        "produced to queryable (raw_events): 3 rows, p50 900 ms, p95 900 ms, p99 900 ms, max 900 ms"
    )


def test_an_empty_run_has_no_latencies(tmp_path: Path) -> None:
    result = latencies(tmp_path)
    assert (result.produced, result.event, result.window, result.batches) == (None, None, None, 0)
    assert result.lines()[1] == "produced to queryable (raw_events): no rows"


def report(batch: int, rows: int, start: str, ms: int) -> str:
    return json.dumps(
        {"batchId": batch, "numInputRows": rows, "timestamp": start, "durationMs": {"triggerExecution": ms}}
    )


def test_throughput_from_the_progress_reports(tmp_path: Path) -> None:
    paths = Paths(tmp_path)
    assert throughput(tmp_path, "raw_events") is None
    paths.progress("raw_events").parent.mkdir(parents=True)
    lines = [
        report(0, 3_000, "2026-10-26T19:00:00.000Z", 1_500),
        report(1, 0, "2026-10-26T19:00:01.500Z", 20),  # an empty batch: neither rows nor time
        report(2, 1_000, "2026-10-26T19:00:02.000Z", 500),
    ]
    paths.progress("raw_events").write_text("\n".join(lines) + "\n")
    result = throughput(tmp_path, "raw_events")
    assert result is not None
    assert (result.batches, result.rows, result.busy_ms, result.span_ms) == (2, 4_000, 2_000, 2_500)
    assert result.rows_per_s == 1_600 and result.rows_per_busy_s == 2_000
    assert result.line() == "raw_events: 4,000 rows in 2 batches over 2.5 s: 1,600 rows/s (2,000 per busy second)"
    paths.progress("dead_letters").write_text(report(0, 0, "2026-10-26T19:00:00.000Z", 5) + "\n")
    assert throughput(tmp_path, "dead_letters") is None


def test_the_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    paths = run(tmp_path)
    paths.progress("raw_events").parent.mkdir(parents=True)
    paths.progress("raw_events").write_text(report(0, 3, "2026-10-26T19:00:00.000Z", 1_000) + "\n")
    assert main(["latency", "--out", str(tmp_path)]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "2 raw_events batches"
    assert out[3] == "window end to queryable (minute_aggregates): 2 rows, p50 185,000 ms, p95 185,000 ms, " + (
        "p99 185,000 ms, max 185,000 ms"
    )
    assert out[4:] == ["throughput:", "  raw_events: 3 rows in 1 batches over 1.0 s: 3 rows/s (3 per busy second)"]
