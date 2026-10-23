import dataclasses

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from flowcast.generator import Config, sessions
from flowcast.schema import Event
from flowcast.truth import Row, duckdb_minute_aggregates, minute_aggregates, window_start
from tests.test_schema import make

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z, a window boundary


def test_windows_start_on_the_minute_from_the_epoch() -> None:
    assert window_start(T0) == T0
    assert window_start(T0 + 59_999) == T0
    assert window_start(T0 + 60_000) == T0 + 60_000
    assert window_start(T0 - 1) == T0 - 60_000


def hand_made() -> list[Event]:
    return [
        make("page_view", event_id="00000000-0000-4000-8000-000000000001", event_time=T0),
        make("page_view", event_id="00000000-0000-4000-8000-000000000002", event_time=T0 + 59_999),
        make("page_view", event_id="00000000-0000-4000-8000-000000000003", event_time=T0 + 60_000),
        make(
            "purchase", event_id="00000000-0000-4000-8000-000000000004", event_time=T0 + 1, price_cents=250, quantity=3
        ),
        make(
            "purchase", event_id="00000000-0000-4000-8000-000000000005", event_time=T0 + 2, price_cents=999, quantity=1
        ),
        # A duplicate of the first purchase: counted once.
        make(
            "purchase", event_id="00000000-0000-4000-8000-000000000004", event_time=T0 + 1, price_cents=250, quantity=3
        ),
        make("add_to_cart", event_id="00000000-0000-4000-8000-000000000006", event_time=T0 + 3),
    ]


EXPECTED = [
    Row(T0, "add_to_cart", 1, 0),
    Row(T0, "page_view", 2, 0),
    Row(T0, "purchase", 2, 750 + 999),
    Row(T0 + 60_000, "page_view", 1, 0),
]


def test_python_truth_on_hand_made_events() -> None:
    assert minute_aggregates(hand_made()) == EXPECTED


def test_duckdb_truth_on_hand_made_events() -> None:
    assert duckdb_minute_aggregates(hand_made()) == EXPECTED


def test_no_events_means_no_rows() -> None:
    assert minute_aggregates([]) == []
    assert duckdb_minute_aggregates([]) == []


def test_the_truth_counts_every_generated_event_once_late_ones_included() -> None:
    records = sessions(2_000)
    rows = minute_aggregates(record.event for record in records)
    unique = {record.event.event_id for record in records}
    assert sum(row.events for row in rows) == len(unique)
    assert any(record.delay == "very_late" for record in records)
    revenue = sum(record.event.revenue_cents() for record in records if not record.duplicate)
    assert sum(row.revenue_cents for row in rows) == revenue > 0


def test_python_and_duckdb_agree_at_the_defaults() -> None:
    events = [record.event for record in sessions(5_000)]
    assert minute_aggregates(events) == duckdb_minute_aggregates(events)


@settings(max_examples=25, deadline=None)
@given(
    seed=st.integers(0, 2**32),
    n=st.integers(1, 300),
    duplicates=st.floats(0, 1),
    rate=st.floats(0.05, 500),
)
def test_python_and_duckdb_agree_for_any_stream(seed: int, n: int, duplicates: float, rate: float) -> None:
    config = Config(seed=seed, duplicate_fraction=duplicates, sessions_per_second=rate)
    events = [record.event for record in sessions(n, config)]
    assert minute_aggregates(events) == duckdb_minute_aggregates(events)


@pytest.mark.parametrize("event_type", ["purchase", "page_view"])
def test_the_largest_values_sum_exactly(event_type: str) -> None:
    # 10,000 lines at the maximum price and quantity: 9.9e12 cents, far past a 32-bit integer; both engines must keep
    # the sum an exact integer.
    base = make("purchase", price_cents=10_000_000, quantity=99, event_time=T0)
    events = [dataclasses.replace(base, event_id=f"00000000-0000-4000-8000-{i:012d}") for i in range(10_000)]
    if event_type == "page_view":
        events = [make("page_view", event_id=e.event_id, event_time=T0) for e in events]
    expected = [Row(T0, event_type, 10_000, 10_000 * 10_000_000 * 99 if event_type == "purchase" else 0)]
    assert minute_aggregates(events) == expected
    assert duckdb_minute_aggregates(events) == expected
