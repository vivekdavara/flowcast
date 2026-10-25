import dataclasses
import json
from pathlib import Path

import duckdb
import pytest

from flowcast.check import (
    Batch,
    CheckpointError,
    check_run,
    committed_batches,
    expected_rows,
    kafka_end_offsets,
    replay,
)
from flowcast.kafka_io import Record
from flowcast.schema import Event, encode
from flowcast.truth import Row
from tests.test_schema import make

T0 = 1_767_225_600_000
TOPIC = "clickstream"


def write_batch(checkpoint: Path, batch_id: int, watermark_ms: int, sources: list[str], *, commit: bool = True) -> None:
    (checkpoint / "offsets").mkdir(parents=True, exist_ok=True)
    (checkpoint / "commits").mkdir(parents=True, exist_ok=True)
    metadata = {"batchWatermarkMs": watermark_ms, "batchTimestampMs": 0, "conf": {}}
    (checkpoint / "offsets" / str(batch_id)).write_text("\n".join(["v1", json.dumps(metadata), *sources]))
    if commit:
        (checkpoint / "commits" / str(batch_id)).write_text('v1\n{"nextBatchWatermarkMs":0}')


def kafka_offsets(**ends: int) -> str:
    return json.dumps({TOPIC: {p.removeprefix("p"): o for p, o in ends.items()}})


def test_committed_batches_reads_each_batchs_watermark_and_offsets(tmp_path: Path) -> None:
    write_batch(tmp_path, 0, 0, [kafka_offsets(p0=3, p1=1)])
    write_batch(tmp_path, 1, T0, [kafka_offsets(p0=5, p1=1)])
    write_batch(tmp_path, 2, T0 + 1, [kafka_offsets(p0=9, p1=9)], commit=False)  # planned, then a crash
    (tmp_path / "offsets" / ".1.crc").write_text("")  # Hadoop's checksum files sit beside the entries
    batches = committed_batches(tmp_path)
    assert [(b.batch_id, b.watermark_ms) for b in batches] == [(0, 0), (1, T0)]
    assert kafka_end_offsets(batches[1], TOPIC) == {0: 5, 1: 1}


def test_a_source_with_no_offset_yet_is_none(tmp_path: Path) -> None:
    write_batch(tmp_path, 0, 0, ["-"])
    assert committed_batches(tmp_path)[0].sources == (None,)
    with pytest.raises(CheckpointError, match="doesn't read the Kafka topic"):
        kafka_end_offsets(committed_batches(tmp_path)[0], TOPIC)


def test_a_checkpoint_missing_its_first_batches_is_refused(tmp_path: Path) -> None:
    # What Spark leaves once it has deleted entries older than minBatchesToRetain: the replay can't start there.
    write_batch(tmp_path, 5, T0, [kafka_offsets(p0=1)])
    with pytest.raises(CheckpointError, match="complete run"):
        committed_batches(tmp_path)


def test_an_unknown_offset_log_version_is_refused(tmp_path: Path) -> None:
    write_batch(tmp_path, 0, 0, [kafka_offsets(p0=1)])
    path = tmp_path / "offsets" / "0"
    path.write_text(path.read_text().replace("v1", "v2", 1))
    with pytest.raises(CheckpointError, match="version-1"):
        committed_batches(tmp_path)


def test_no_commits_is_no_batches(tmp_path: Path) -> None:
    assert committed_batches(tmp_path) == []
    write_batch(tmp_path, 0, 0, [kafka_offsets(p0=1)], commit=False)
    assert committed_batches(tmp_path) == []


def ev(n: int, event_time: int, event_type: str = "page_view", **changes: object) -> Event:
    return make(event_type, event_id=f"00000000-0000-4000-8000-{n:012}", event_time=event_time, **changes)


def rec(partition: int, offset: int, value: Event | bytes) -> Record:
    return Record(partition, offset, b"u42", encode(value) if isinstance(value, Event) else value, 0)


def batch(batch_id: int, watermark_ms: int, **ends: int) -> Batch:
    return Batch(batch_id, watermark_ms, (json.loads(kafka_offsets(**ends)),))


def test_replay_drops_a_record_at_or_before_the_previous_batchs_watermark() -> None:
    w1 = T0 - 180_000
    records = [
        rec(0, 0, ev(1, T0)),
        rec(0, 1, ev(2, T0 - 200_000)),  # batch 1: below batch 1's own watermark, but its filter is batch 0's (0)
        rec(1, 0, ev(3, w1)),  # batch 2: exactly on batch 1's watermark: dropped
        rec(1, 1, ev(4, w1 + 1)),  # batch 2: kept
        rec(0, 2, b"not json"),  # batch 2: a dead letter
        rec(1, 2, ev(5, T0)),  # never read: past the last batch's end offsets
    ]
    batches = [batch(0, 0, p0=1, p1=0), batch(1, w1, p0=2, p1=0), batch(2, w1, p0=3, p1=2)]
    result = replay(records, batches, TOPIC)
    assert [e.event_id[-1] for e in result.kept] == ["1", "2", "4"]
    assert [e.event_id[-1] for e in result.dropped] == ["3"]
    assert (result.invalid, result.unread, result.watermark_ms) == (1, 1, w1)


def test_replay_takes_records_in_any_order() -> None:
    records = [rec(0, 1, ev(2, T0 - 200_000)), rec(0, 0, ev(1, T0))]
    batches = [batch(0, 0, p0=1), batch(1, T0 - 180_000, p0=2)]
    assert [e.event_id[-1] for e in replay(records, batches, TOPIC).kept] == ["1", "2"]


def test_a_no_data_batch_moves_the_late_watermark_on() -> None:
    # Batch 1 reads nothing (its end offsets are batch 0's) but runs with a new watermark, which batch 2 filters by.
    w1 = T0 - 180_000
    records = [rec(0, 0, ev(1, T0)), rec(0, 1, ev(2, w1 - 1))]
    batches = [batch(0, 0, p0=1), batch(1, w1, p0=1), batch(2, w1, p0=2)]
    result = replay(records, batches, TOPIC)
    assert [e.event_id[-1] for e in result.dropped] == ["2"]


def test_with_no_batches_every_record_is_unread() -> None:
    result = replay([rec(0, 0, ev(1, T0))], [], TOPIC)
    assert (result.kept, result.dropped, result.unread, result.watermark_ms) == ([], [], 1, 0)


def test_expected_rows_are_the_truth_of_kept_records_in_windows_the_watermark_has_passed() -> None:
    w = T0 + 60_000
    records = [
        rec(0, 0, ev(1, T0, "purchase", price_cents=250, quantity=3)),
        rec(0, 1, ev(1, T0, "purchase", price_cents=250, quantity=3)),  # a duplicate: once
        rec(0, 2, ev(2, T0 + 59_999)),
        rec(0, 3, ev(3, T0 + 60_000)),  # its window ends at T0 + 2 min, after the watermark: not written yet
    ]
    result = replay(records, [batch(0, 0, p0=4), batch(1, w, p0=4)], TOPIC)
    assert expected_rows(result) == [Row(T0, "page_view", 1, 0), Row(T0, "purchase", 1, 750)]
    later = dataclasses.replace(result, watermark_ms=T0 + 120_000)
    assert Row(T0 + 60_000, "page_view", 1, 0) in expected_rows(later)


def test_each_batchs_watermark_is_recomputed_from_the_records_before_it() -> None:
    records = [rec(0, 0, ev(1, T0)), rec(0, 1, ev(2, T0 + 60_000)), rec(0, 2, ev(3, T0 - 600_000))]
    right = [
        batch(0, 0, p0=1),
        batch(1, T0 - 180_000, p0=2),
        batch(2, T0 - 120_000, p0=3),
        batch(3, T0 - 120_000, p0=3),
    ]
    assert replay(records, right, TOPIC).watermark_errors == []
    # A job run with a 2-minute delay: its own checkpoint is self-consistent, but not with the specified 3 minutes.
    two_minutes = [batch(0, 0, p0=1), batch(1, T0 - 120_000, p0=2), batch(2, T0 - 60_000, p0=3)]
    assert replay(records, two_minutes, TOPIC).watermark_errors == [
        f"batch 1: watermark {T0 - 120_000}, expected {T0 - 180_000}",
        f"batch 2: watermark {T0 - 60_000}, expected {T0 - 120_000}",
    ]
    assert replay(records, two_minutes, TOPIC, delay_ms=120_000).watermark_errors == []


def test_the_watermark_ignores_invalid_records_and_never_moves_back() -> None:
    records = [rec(0, 0, ev(1, T0)), rec(0, 1, b"{"), rec(0, 2, ev(2, T0 - 900_000))]
    batches = [batch(0, 0, p0=2), batch(1, T0 - 180_000, p0=3), batch(2, T0 - 180_000, p0=3)]
    assert replay(records, batches, TOPIC).watermark_errors == []


def test_an_offset_log_entry_without_its_metadata_line_is_refused(tmp_path: Path) -> None:
    write_batch(tmp_path, 0, 0, [])
    (tmp_path / "offsets" / "0").write_text("v1")
    with pytest.raises(CheckpointError, match="version-1"):
        committed_batches(tmp_path)


def test_a_batch_reading_two_sources_is_not_a_kafka_batch() -> None:
    two = Batch(0, 0, (json.loads(kafka_offsets(p0=1)), json.loads(kafka_offsets(p0=1))))
    with pytest.raises(CheckpointError, match="doesn't read the Kafka topic"):
        kafka_end_offsets(two, TOPIC)


def test_a_partition_missing_from_a_batchs_offsets_was_not_read() -> None:
    result = replay([rec(0, 0, ev(1, T0)), rec(1, 0, ev(2, T0))], [batch(0, 0, p0=1)], TOPIC)
    assert ([e.event_id[-1] for e in result.kept], result.unread) == (["1"], 1)


# check_run over a hand-built run directory (sinks written by DuckDB, Kafka stubbed): no Spark, no broker.


def sink_log(sink: Path, batch_id: int, files: list[Path]) -> None:
    log = sink / "_spark_metadata"
    log.mkdir(parents=True, exist_ok=True)
    entries = [json.dumps({"path": f.as_uri(), "size": 1, "isDir": False, "action": "add"}) for f in files]
    (log / str(batch_id)).write_text("\n".join(["v1", *entries]))


def copy_to(path: Path, select: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute(f"COPY ({select}) TO '{path}'")
    return path


def fake_run(root: Path, aggregate_rows: list[Row], raw_rows: int, dead_rows: int, w1: int = T0 - 180_000) -> None:
    """Records p0:0-3 valid, p0:4 invalid. The aggregate query read p0 up to 3 in two batches (batch 1 with watermark
    `w1`); the raw and dead letter queries read all five in one."""
    write_batch(root / "_checkpoints" / "minute_aggregates", 0, 0, [kafka_offsets(p0=1)])
    write_batch(root / "_checkpoints" / "minute_aggregates", 1, w1, [kafka_offsets(p0=3)])
    write_batch(root / "_checkpoints" / "raw_events", 0, 0, [kafka_offsets(p0=5)])
    write_batch(root / "_checkpoints" / "dead_letters", 0, 0, [kafka_offsets(p0=5)])
    rows = " UNION ALL ".join(
        f"SELECT {r.window_start}::BIGINT AS window_start, '{r.event_type}' AS event_type,"
        f" {r.events}::BIGINT AS events, {r.revenue_cents}::BIGINT AS revenue_cents"
        for r in aggregate_rows
    )
    aggregates = [copy_to(root / "minute_aggregates" / "part-0.parquet", rows)] if aggregate_rows else []
    sink_log(root / "minute_aggregates", 0, [])
    sink_log(root / "minute_aggregates", 1, aggregates)
    raw = copy_to(
        root / "raw_events" / "event_date=2026-01-01" / "part-0.parquet", f"SELECT range AS n FROM range({raw_rows})"
    )
    sink_log(root / "raw_events", 0, [raw])
    dead = copy_to(root / "dead_letters" / "part-0.parquet", f"SELECT range AS n FROM range({dead_rows})")
    sink_log(root / "dead_letters", 0, [dead])


RUN_RECORDS = [
    rec(0, 0, ev(1, T0, "purchase", price_cents=100, quantity=2)),
    rec(0, 1, ev(2, T0 - 300_000)),  # batch 1: below W1, but batch 1 filters by batch 0's watermark (0): kept
    rec(0, 2, ev(3, T0 - 179_000)),
    rec(0, 3, ev(4, T0 + 10)),  # not read by the aggregate query
    rec(0, 4, b"{"),
]


@pytest.fixture
def stub_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    import flowcast.kafka_io

    monkeypatch.setattr(flowcast.kafka_io, "read_topic", lambda *_args: list(RUN_RECORDS))


@pytest.mark.usefixtures("stub_kafka")
def test_check_run_reconciles_each_output_with_its_own_querys_batches(tmp_path: Path) -> None:
    expected = [Row(T0 - 300_000, "page_view", 1, 0)]  # the one window ending at or before W1 = T0 - 3 min
    fake_run(tmp_path, expected, raw_rows=4, dead_rows=1)
    report = check_run("unused", TOPIC, tmp_path)
    assert (report.batches, report.records, report.replay.unread) == (2, 5, 2)
    assert report.expected == report.written == expected
    assert (report.raw_events, report.dead_letters) == ((4, 4), (1, 1))
    assert report.ok and report.lines()[-1] == "ok"


@pytest.mark.usefixtures("stub_kafka")
@pytest.mark.parametrize(
    ("rows", "raw", "dead"),
    [([], 4, 1), ([Row(T0 - 300_000, "page_view", 2, 0)], 4, 1), (None, 3, 1), (None, 4, 0)],
)
def test_check_run_fails_on_any_output_that_doesnt_reconcile(
    tmp_path: Path, rows: list[Row] | None, raw: int, dead: int
) -> None:
    fake_run(tmp_path, [Row(T0 - 300_000, "page_view", 1, 0)] if rows is None else rows, raw, dead)
    report = check_run("unused", TOPIC, tmp_path)
    assert not report.ok and report.lines()[-1] == "MISMATCH"


@pytest.mark.parametrize("sink_batch", [0, 2])
def test_check_run_refuses_a_sink_and_checkpoint_on_different_batches(tmp_path: Path, sink_batch: int) -> None:
    fake_run(tmp_path, [Row(T0 - 300_000, "page_view", 1, 0)], 4, 1)
    log = tmp_path / "minute_aggregates" / "_spark_metadata"
    if sink_batch == 0:
        (log / "1").unlink()  # the checkpoint committed batch 1, the sink never logged it
    else:
        sink_log(tmp_path / "minute_aggregates", 2, [])  # the sink logged batch 2, the checkpoint hasn't committed it
    with pytest.raises(CheckpointError, match="disagree on the last batch"):
        check_run("unused", TOPIC, tmp_path)


@pytest.mark.usefixtures("stub_kafka")
def test_check_run_fails_a_job_run_with_another_watermark_delay(tmp_path: Path) -> None:
    # A 4-minute delay: the same rows are written (the window ends at T0 - 4 min either way), but the job isn't the
    # one specified.
    fake_run(tmp_path, [Row(T0 - 300_000, "page_view", 1, 0)], 4, 1, w1=T0 - 240_000)
    report = check_run("unused", TOPIC, tmp_path)
    assert report.written == report.expected
    assert not report.ok
    assert f"  batch 1: watermark {T0 - 240_000}, expected {T0 - 180_000}" in report.lines()


@pytest.mark.usefixtures("stub_kafka")
def test_check_run_counts_orphan_files_without_failing(tmp_path: Path) -> None:
    # A raw-events file of a batch that failed before it was logged: never read, so the run still reconciles.
    fake_run(tmp_path, [Row(T0 - 300_000, "page_view", 1, 0)], 4, 1)
    copy_to(tmp_path / "raw_events" / "event_date=2026-01-01" / "part-1.parquet", "SELECT 1 AS n")
    report = check_run("unused", TOPIC, tmp_path)
    assert report.orphans == {"minute_aggregates": 0, "raw_events": 1, "dead_letters": 0}
    assert "orphan files (not in any sink log, never read): minute_aggregates 0, raw_events 1, dead_letters 0" in (
        report.lines()
    )
    assert report.ok
