import dataclasses
import json
from pathlib import Path

import pytest

from flowcast.check import (
    Batch,
    CheckpointError,
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
