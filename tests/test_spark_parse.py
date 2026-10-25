"""The job's parse step against the reference decoder: the same verdict, and the same reason, for every record."""

import dataclasses
import datetime
from typing import Any

import pytest

from flowcast.generator import Config, sessions
from flowcast.schema import FIELDS, InvalidEvent, decode, encode
from flowcast.spark_job import safe_reason
from tests.test_schema import make, raw

pytestmark = pytest.mark.spark

KAFKA_SCHEMA = "value BINARY, topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP"


def records() -> list[bytes | None]:
    good = [encode(r.event) for r in sessions(40)]
    good += [encode(r.event) for r in sessions(20, Config(seed=2, schema_version=2))]
    good.append(raw(make(schema_version=2)).replace(b', "campaign": null', b""))  # version 2 leaving campaign out
    bad: list[bytes | None] = [
        None,  # a tombstone
        b"",
        b"\xff\xfe",
        b"{",
        b"[]",
        b"null",
        b"[" * 100_000,
        raw(make(), schema_version=True),
        raw(make(), event_time=1_767_225_600_000.0),
        raw(make(), event_time="1767225600000"),
        raw(make(), user_id="u٤٢"),
        raw(make(), page="/café"),
        raw(make(), quantity=2.0),
        raw(make("page_view"), quantity=1),
        raw(make(), extra=1),
        encode(make())[:-1] + b',"quantity":3}',  # a duplicate key: from_json would keep the last
        encode(make()).replace(b'"price_cents":1999', b'"price_cents":NaN'),
        encode(make()).replace(b'"quantity":2', b'"quantity":true'),
        b'{"\\ud800":1,"\\ud800":2}',  # the reason quotes a key that is a lone surrogate (day-2 review)
        encode(make())[:-1] + b',"\\udfff":1}',
        raw(make(), campaign="spring_sale"),  # campaign in a version-1 record
        raw(make(schema_version=2), campaign="Spring Sale"),
        raw(make(schema_version=3)),
    ]
    return good + bad


def verdict(value: bytes | None) -> tuple[Any, ...]:
    try:
        return (None, *dataclasses.astuple(decode(value)))
    except InvalidEvent as exc:
        return (safe_reason(exc), *(None,) * len(FIELDS))


def test_parse_gives_decodes_verdict_and_reason_for_every_record(spark: Any) -> None:
    from flowcast.spark_job import parse

    values = records()
    when = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    frame = spark.createDataFrame([(v, "t", 0, i, when) for i, v in enumerate(values)], KAFKA_SCHEMA)
    rows = sorted(parse(frame).collect(), key=lambda r: r.offset)
    assert [r.offset for r in rows] == list(range(len(values)))
    for row, value in zip(rows, values, strict=True):
        assert tuple(row[name] for name in ("error", *FIELDS)) == verdict(value), value
        assert (row.value is None and value is None) or bytes(row.value) == value
    assert sum(r.error is not None for r in rows) == 23
    assert rows[values.index(b'{"\\ud800":1,"\\ud800":2}')].error == "duplicate field \\ud800"


def test_dead_letters_and_raw_events_split_the_records(spark: Any) -> None:
    from flowcast.spark_job import dead_letters, parse, raw_events

    values = records()
    when = datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.UTC)
    frame = spark.createDataFrame([(v, "t", i % 3, i, when) for i, v in enumerate(values)], KAFKA_SCHEMA)
    parsed = parse(frame)
    letters = dead_letters(parsed).collect()
    events = raw_events(parsed).collect()
    assert len(letters) + len(events) == len(values)
    assert dead_letters(parsed).columns == ["topic", "partition", "offset", "kafka_timestamp", "value", "error"]
    assert raw_events(parsed).columns == [*FIELDS, "topic", "partition", "offset", "kafka_timestamp", "event_date"]
    letter = next(r for r in letters if r.offset == values.index(b"{"))
    assert (letter.topic, letter.partition, bytes(letter.value), letter.error) == (
        "t",
        letter.offset % 3,
        b"{",
        "not JSON",
    )
    # PySpark hands a timestamp back naive and in this process's local zone (not the session's UTC): the same instant.
    assert letter.kafka_timestamp == when.astimezone().replace(tzinfo=None)
    for row in events:
        expected = datetime.datetime.fromtimestamp(row.event_time / 1000, datetime.UTC).date()
        assert row.event_date == expected
