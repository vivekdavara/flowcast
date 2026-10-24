"""Review of day 2: inputs the parse step must turn into dead letters rather than a crashed query."""

import datetime
from typing import Any

import pytest

from tests.test_schema import make, raw

pytestmark = pytest.mark.spark

KAFKA_SCHEMA = "value BINARY, topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP"


@pytest.mark.parametrize(
    "value",
    [
        b'{"\\ud800":1,"\\ud800":2}',  # decode: "duplicate field \ud800" (a lone surrogate in the reason)
        raw(make())[:-1] + b',"\\udfff":1}',  # decode: "unknown field \udfff"
    ],
)
def test_a_record_whose_reason_holds_a_lone_surrogate_is_a_dead_letter(spark: Any, value: bytes) -> None:
    from flowcast.spark_job import dead_letters, parse

    when = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    frame = spark.createDataFrame([(value, "t", 0, 0, when)], KAFKA_SCHEMA)
    # The reason can't be stored as Spark's UTF-8 string as is, but the record must still come out as a dead letter
    # (any readable reason): one bad record on the topic must not fail the task, and with it the streaming query.
    letters = dead_letters(parse(frame)).collect()
    assert len(letters) == 1 and letters[0].error
