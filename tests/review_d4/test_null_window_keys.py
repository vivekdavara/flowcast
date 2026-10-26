"""A minute_aggregates row with a NULL event_type or window_start broke no row rule: `NULL NOT IN (...)` and
`NULL % 60000 <> 0` are NULL, so the row wasn't counted (day-4 review). Its own required-fields rule catches it."""

import duckdb
import pytest

from flowcast.quality import run_rules
from tests.test_quality import aggregate, clean, tables


@pytest.mark.parametrize("row", [aggregate(event_type=None), aggregate(window_start=None)])
def test_a_null_window_key_fails_required_fields(row: dict[str, object]) -> None:
    con = duckdb.connect()
    data = clean()
    data["minute_aggregates"].append(row)
    tables(con, data)
    failed = {(r.view, r.name) for r in run_rules(con).results if not r.ok}
    assert ("minute_aggregates", "required fields set") in failed
