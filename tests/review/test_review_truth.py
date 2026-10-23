"""Review of 943596b: truth.py's Python aggregate against sql/minute_aggregates.sql."""

import dataclasses

from flowcast.truth import duckdb_minute_aggregates, minute_aggregates
from tests.test_schema import make

T0 = 1_767_225_600_000


def test_both_engines_keep_the_same_copy_of_a_duplicated_event_id() -> None:
    # Python keeps the first copy of an event_id; `SELECT DISTINCT ON (event_id) *` with no ORDER BY keeps an arbitrary
    # one, and DuckDB's parallel aggregation picks the later copy here. With copies that differ (another window), the
    # two "same definition" truths disagree: `flowcast truth --input f` gives different CSV per --engine.
    n = 200_000  # big enough for DuckDB to aggregate in parallel
    first = make("page_view", event_time=T0)
    later = make("page_view", event_time=T0 + 60_000)
    ids = [f"00000000-0000-4000-8000-{i:012d}" for i in range(n)]
    events = [dataclasses.replace(first, event_id=i) for i in ids]
    events += [dataclasses.replace(later, event_id=i) for i in ids]
    assert duckdb_minute_aggregates(events) == minute_aggregates(events)
