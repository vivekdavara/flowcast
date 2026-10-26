"""Out-of-range values that the row rules exist to report crash run_rules instead: the cross-check's
price_cents * quantity (BIGINT) and the event-date rule's event_time * 1000 overflow, and DuckDB raises."""

import duckdb

from flowcast.quality import run_rules
from tests.test_quality import clean, raw, tables


def _report(row):
    con = duckdb.connect()
    data = clean()
    data["raw_events"].append(row)
    tables(con, data)
    return run_rules(con)  # expected: a report with the range rule failing; actual: duckdb.OutOfRangeException


def test_huge_price_is_reported_not_raised():
    report = _report(raw(500, price_cents=2**62))
    assert any(r.name == "price and quantity in range" and not r.ok for r in report.results)


def test_huge_event_time_is_reported_not_raised():
    report = _report(raw(500, event_time=10**16))
    assert any(r.name == "event time in range" and not r.ok for r in report.results)
