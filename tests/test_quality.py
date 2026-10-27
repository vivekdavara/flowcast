"""The data-quality rules (quality.py): a clean run passes, and each rule catches the one fault it is for."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from flowcast.cli import main
from flowcast.generator import Config, take
from flowcast.quality import ROW_RULES, SET_RULES, check_quality, run_rules
from flowcast.schema import (
    DEVICES,
    EVENT_TYPES,
    FIELDS,
    MAX_EVENT_TIME,
    MAX_PRICE_CENTS,
    MAX_QUANTITY,
    MIN_EVENT_TIME,
    STRING_FORMATS,
    V1_FIELDS,
    InvalidEvent,
    decode,
    values,
)
from flowcast.views import COLUMNS
from tests.test_views import log_file

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def raw(offset: int, event_type: str = "purchase", event_time: int = T0, **changes: object) -> dict[str, object]:
    product = event_type in ("product_view", "add_to_cart", "remove_from_cart", "purchase")
    quantity = event_type in ("add_to_cart", "remove_from_cart", "purchase")
    row: dict[str, object] = {
        "schema_version": 1,
        "event_id": f"00000000-0000-4000-8000-{offset:012d}",
        "event_type": event_type,
        "event_time": event_time,
        "user_id": "u1",
        "session_id": "s1",
        "device": "desktop",
        "country": "US",
        "page": "/cart",
        "product_id": "p1" if product else None,
        "category": "shoes" if product else None,
        "price_cents": 500 if product else None,
        "quantity": 2 if quantity else None,
        "campaign": None,
        "topic": "clicks",
        "partition": 0,
        "offset": offset,
        "kafka_timestamp": "2026-01-01 00:00:01+00",
        "event_date": "2026-01-01",
    }
    return {**row, **changes}


def aggregate(window_start: int = T0, event_type: str = "purchase", events: int = 1, revenue: int = 1000) -> dict:
    return {"window_start": window_start, "event_type": event_type, "events": events, "revenue_cents": revenue}


def dead(offset: int, error: str | None = "not JSON") -> dict[str, object]:
    return {
        "topic": "clicks",
        "partition": 0,
        "offset": offset,
        "kafka_timestamp": "2026-01-01 00:00:01+00",
        "value": b"{",
        "error": error,
    }


def tables(con: duckdb.DuckDBPyConnection, data: dict[str, list[dict[str, object]]]) -> None:
    """The three views' tables, with the views' own column types, holding `data`."""
    for view in ("raw_events", "minute_aggregates", "dead_letters"):
        columns = COLUMNS[view]
        definitions = ", ".join(f'"{c}" {t}' for c, t in columns.items())
        con.execute(f"CREATE TABLE {view} ({definitions})")
        for row in data.get(view, []):
            con.execute(f"INSERT INTO {view} VALUES ({', '.join('?' for _ in columns)})", [row[c] for c in columns])


def clean() -> dict[str, list[dict[str, object]]]:
    """A run that passes: a purchase and its duplicate, a page view the next minute, 100 more, one dead letter."""
    events = [raw(0), raw(1, event_id=raw(0)["event_id"]), raw(2, "page_view", T0 + 60_000)]
    events += [raw(3 + i, "page_view", T0 + 120_000) for i in range(100)]
    return {
        "raw_events": events,
        "minute_aggregates": [
            aggregate(),
            aggregate(T0 + 60_000, "page_view", 1, 0),
            aggregate(T0 + 120_000, "page_view", 100, 0),
        ],
        "dead_letters": [dead(103)],
    }


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    yield connection
    connection.close()


def failing(report_lines: list[str]) -> list[str]:
    return [line for line in report_lines if line.startswith("FAIL")]


def test_a_clean_run_passes(con: duckdb.DuckDBPyConnection) -> None:
    tables(con, clean())
    report = run_rules(con)
    assert report.ok, report.lines()
    assert report.rows == {"raw_events": 103, "minute_aggregates": 3, "dead_letters": 1}
    assert len(report.results) == len(ROW_RULES) + len(SET_RULES)
    assert report.lines()[-1] == "quality: ok"
    assert report.dead_letter_rate == pytest.approx(1 / 104)


def test_an_empty_run_passes(con: duckdb.DuckDBPyConnection) -> None:
    tables(con, {})
    report = run_rules(con)
    assert report.ok and report.dead_letter_rate == 0.0


# A broken raw event is added as a record of its own (offset 500); a broken window replaces the first one. A window
# that no raw event backs (an unknown type, a time off the minute, revenue from page views) also breaks the
# cross-check, as it should: CROSS is that rule.
CROSS = "no window counts more than the raw events"
BROKEN = [
    ("raw_events", raw(500, event_id=None), ["required fields set"]),
    ("raw_events", raw(500, kafka_timestamp=None), ["required fields set"]),
    ("raw_events", raw(500, "click"), ["known event type"]),
    ("raw_events", raw(500, device="watch"), ["known device"]),
    ("raw_events", raw(500, schema_version=3), ["known schema version"]),
    ("raw_events", raw(500, campaign="fall_sale"), ["known schema version"]),
    ("raw_events", raw(500, event_time=4_102_444_800_000, event_date="2100-01-01"), ["event time in range"]),
    ("raw_events", raw(500, "page_view", product_id="p1"), ["product fields match"]),
    ("raw_events", raw(500, "product_view", quantity=1), ["product fields match"]),
    ("raw_events", raw(500, price_cents=None), ["product fields match"]),
    ("raw_events", raw(500, price_cents=0), ["price and quantity in range"]),
    ("raw_events", raw(500, quantity=100), ["price and quantity in range"]),
    ("raw_events", raw(500, event_date="2025-12-31"), ["event date"]),
    # The decoder's string formats (day-4 review): each a value one character from passing.
    ("raw_events", raw(500, event_id="00000000-0000-4000-8000-00000000050A"), ["event_id in the decoder's format"]),
    ("raw_events", raw(500, user_id="U1"), ["user_id in the decoder's format"]),
    ("raw_events", raw(500, session_id="s" + "1" * 32), ["session_id in the decoder's format"]),
    ("raw_events", raw(500, country="USA"), ["country in the decoder's format"]),
    ("raw_events", raw(500, page="/my cart"), ["page in the decoder's format"]),
    ("raw_events", raw(500, page="cart"), ["page in the decoder's format"]),
    # A space at either end (day-5 review: a rule matching trim(page) passed every other case).
    ("raw_events", raw(500, page="/cart "), ["page in the decoder's format"]),
    ("raw_events", raw(500, user_id=" u1"), ["user_id in the decoder's format"]),
    ("raw_events", raw(500, product_id="p"), ["product_id in the decoder's format"]),
    ("raw_events", raw(500, category="Shoes"), ["category in the decoder's format"]),
    ("raw_events", raw(500, schema_version=2, campaign="Fall"), ["campaign in the decoder's format"]),
    ("minute_aggregates", aggregate(event_type="click"), ["known event type", "counts positive", CROSS]),
    ("minute_aggregates", aggregate(T0 + 1), ["window starts on a minute", CROSS]),
    ("minute_aggregates", aggregate(events=0), ["counts positive"]),
    ("minute_aggregates", aggregate(revenue=-1), ["counts positive"]),
    ("minute_aggregates", aggregate(event_type="page_view", revenue=5), ["counts positive", CROSS]),
    ("dead_letters", dead(103, None), ["dead letters have a reason"]),
    ("dead_letters", dead(103, ""), ["dead letters have a reason"]),
]


@pytest.mark.parametrize(("view", "row", "rules"), BROKEN)
def test_each_row_rule_catches_its_fault(
    con: duckdb.DuckDBPyConnection, view: str, row: dict[str, object], rules: list[str]
) -> None:
    data = clean()
    if view == "raw_events":
        data[view].append(row)
    else:
        data[view][0] = row
    tables(con, data)
    report = run_rules(con)
    assert not report.ok
    fails = failing(report.lines())
    # These rules fail, and no others.
    assert len(fails) == len(rules), fails
    for rule in rules:
        assert any(rule in line for line in fails), (rule, fails)
    assert any(f"{view}: {rules[0]}" in line for line in fails)


def test_a_record_twice_or_as_both_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    data = clean()
    data["raw_events"].append(raw(5))  # offset 5 again
    data["dead_letters"].append(dead(7))  # offset 7 is also an event
    tables(con, data)
    report = run_rules(con)
    result = next(r for r in report.results if r.name.startswith("each Kafka record once"))
    assert result.failures == 2
    assert result.samples == (("clicks", 0, 5, 2), ("clicks", 0, 7, 2))
    assert "2 rows, e.g. clicks 0 5 2; clicks 0 7 2" in "\n".join(report.lines())


def test_a_window_twice_is_caught(con: duckdb.DuckDBPyConnection) -> None:
    data = clean()
    data["minute_aggregates"].append(aggregate(T0 + 60_000, "page_view", 1, 0))
    tables(con, data)
    fails = failing(run_rules(con).lines())
    assert len(fails) == 1 and "each window and event type once: 1 rows" in fails[0]


@pytest.mark.parametrize(
    "change",
    [
        aggregate(T0 + 120_000, "page_view", 101, 0),  # one more event than raw holds (100)
        aggregate(revenue=1001),  # more revenue than the purchase's 500 x 2
        aggregate(T0 + 180_000, "page_view", 1, 0),  # a window with no raw events at all
    ],
)
def test_a_window_cant_count_more_than_raw(con: duckdb.DuckDBPyConnection, change: dict[str, object]) -> None:
    data = clean()
    keep = [
        a
        for a in data["minute_aggregates"]
        if (a["window_start"], a["event_type"]) != (change["window_start"], change["event_type"])
    ]
    data["minute_aggregates"] = [*keep, change]
    tables(con, data)
    fails = failing(run_rules(con).lines())
    assert len(fails) == 1 and "no window counts more than the raw events" in fails[0], fails


def test_a_window_may_count_fewer_than_raw(con: duckdb.DuckDBPyConnection) -> None:
    """Late records the stream dropped are in raw_events but not in their window."""
    data = clean()
    data["minute_aggregates"][2] = aggregate(T0 + 120_000, "page_view", 60, 0)
    tables(con, data)
    assert run_rules(con).ok


def test_the_dead_letter_rate_has_a_ceiling(con: duckdb.DuckDBPyConnection) -> None:
    data = clean()
    data["dead_letters"] += [dead(200 + i) for i in range(2)]  # 3 of 106 records
    tables(con, data)
    assert not run_rules(con).ok
    assert run_rules(con).lines()[-2].startswith("FAIL  dead_letters: 2.8302% of records (at most 1.00%)")
    assert run_rules(con, max_dead_letter_rate=0.05).ok
    with pytest.raises(ValueError, match="between 0 and 1"):
        run_rules(con, max_dead_letter_rate=1.5)


def parquet(con: duckdb.DuckDBPyConnection, view: str, sink: Path, name: str, partition: str | None = None) -> None:
    """The table `view` as one Parquet file in a sink directory, logged as batch 0 (the job's layout)."""
    folder = sink / partition if partition else sink
    folder.mkdir(parents=True)
    path = folder / f"part-{name}.parquet"
    columns = "* EXCLUDE (event_date)" if partition else "*"
    con.execute(f"COPY (SELECT {columns} FROM {view}) TO '{path}' (FORMAT parquet)")
    log_file(sink, "0", [(path, "add")])


def test_check_quality_reads_a_runs_committed_files(con: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
    data = clean()
    data["raw_events"][2] = raw(2, "page_view", T0 + 60_000, device="watch")
    tables(con, data)
    parquet(con, "raw_events", tmp_path / "raw_events", "0", "event_date=2026-01-01")
    parquet(con, "minute_aggregates", tmp_path / "minute_aggregates", "0")
    parquet(con, "dead_letters", tmp_path / "dead_letters", "0")
    report = check_quality(tmp_path)
    assert report.rows == {"raw_events": 103, "minute_aggregates": 3, "dead_letters": 1}
    assert failing(report.lines()) == ["FAIL  raw_events: known device: 1 rows, e.g. clicks 0 2"]


def test_the_command(con: duckdb.DuckDBPyConnection, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["quality", "--out", str(tmp_path)]) == 0  # nothing written yet: nothing broken
    assert capsys.readouterr().out.splitlines()[-1] == "quality: ok"
    tables(con, {"raw_events": [raw(0)], "dead_letters": [dead(1)]})
    parquet(con, "raw_events", tmp_path / "raw_events", "0", "event_date=2026-01-01")
    parquet(con, "dead_letters", tmp_path / "dead_letters", "0")
    assert main(["quality", "--out", str(tmp_path)]) == 1
    out, err = capsys.readouterr()
    assert "FAIL  dead_letters: 50.0000% of records" in out
    assert err == "flowcast quality: the outputs break 1 data-quality rule\n"
    assert main(["quality", "--out", str(tmp_path), "--max-dead-letter-rate", "0.5"]) == 0
    capsys.readouterr()
    assert main(["quality", "--out", str(tmp_path), "--max-dead-letter-rate", "2"]) == 2


# Values for each raw-event column, valid or not but always storable in it: the decoder's formats, strings one step
# from them (case, length, a space, DEL, a non-ASCII digit, a newline), the ranges' edges, and NULL.
_TEXT = st.text(alphabet=st.sampled_from("ups09azAZ_-/~ \x7f\n\x00٣é"), max_size=6)
_INT32 = st.integers(-(2**31), 2**31 - 1)
_INT64 = st.integers(-(2**63), 2**63 - 1)


def _string(pattern: str, *edges: str) -> st.SearchStrategy[str | None]:
    return st.one_of(st.from_regex(pattern, fullmatch=True), _TEXT, st.sampled_from(edges), st.none())


_VALUES: dict[str, st.SearchStrategy[object]] = {
    "schema_version": st.one_of(st.sampled_from([0, 1, 2, 3]), _INT32, st.none()),
    "event_type": st.one_of(st.sampled_from([*EVENT_TYPES, "click", "Purchase"]), st.none()),
    "device": st.one_of(st.sampled_from([*DEVICES, "watch", ""]), st.none()),
    "event_time": st.one_of(
        st.sampled_from([MIN_EVENT_TIME - 1, MIN_EVENT_TIME, MAX_EVENT_TIME - 1, MAX_EVENT_TIME]), _INT64, st.none()
    ),
    "price_cents": st.one_of(st.sampled_from([0, 1, MAX_PRICE_CENTS, MAX_PRICE_CENTS + 1]), _INT64, st.none()),
    "quantity": st.one_of(st.sampled_from([0, 1, MAX_QUANTITY, MAX_QUANTITY + 1]), _INT32, st.none()),
    "event_id": _string(STRING_FORMATS["event_id"].pattern, "00000000-0000-4000-8000-00000000000A"),
    "user_id": _string("u[0-9]{1,31}", "u", "u" + "1" * 32, "U1", " u1", "u1 "),
    "session_id": _string("s[0-9]{1,31}", "s", "s" + "1" * 32),
    "country": _string("[A-Z]{2}", "U", "USA", "us"),
    "page": _string("/[\x21-\x7e]{0,8}", "/", "/" + "a" * 255, "/" + "a" * 256, "cart", " /cart", "/cart\n"),
    "product_id": _string("p[0-9]{1,31}", "p", "p" + "1" * 32),
    "category": _string("[a-z_]{1,32}", "", "a" * 32, "a" * 33),
    "campaign": _string("[a-z0-9_-]{1,64}", "", "a" * 64, "a" * 65),
}


def _event_date(event_time: object) -> str:
    if isinstance(event_time, int) and MIN_EVENT_TIME <= event_time < MAX_EVENT_TIME:
        return datetime.fromtimestamp(event_time / 1000, UTC).date().isoformat()
    return "2026-01-01"  # the date rule only judges times in range


@st.composite
def _raw_event(draw: st.DrawFn) -> dict[str, object]:
    """A valid generated event with up to three of its fields replaced."""
    base = dict(zip(FIELDS, values(draw(st.sampled_from(EVENTS))), strict=True))
    for field in draw(st.lists(st.sampled_from(sorted(_VALUES)), max_size=3, unique=True)):
        base[field] = draw(_VALUES[field])
    return base


EVENTS = [record.event for record in take(400, Config(seed=7, schema_version=2))]


@pytest.fixture(scope="module")
def empty_run() -> Iterator[duckdb.DuckDBPyConnection]:
    """One connection for every example: a connection each took most of the time."""
    connection = duckdb.connect()
    tables(connection, {})
    yield connection
    connection.close()


@settings(max_examples=300, deadline=None)
@given(fields=_raw_event())
def test_a_raw_event_passes_the_rules_exactly_when_the_decoder_accepts_it(
    empty_run: duckdb.DuckDBPyConnection, fields: dict[str, object]
) -> None:
    """The row rules restate the decoder over the views' columns: no rule may pass a record the decoder refuses
    (the day-4 review found the string formats missing), and none may fail one it accepts."""
    keys = [*V1_FIELDS, "campaign"] if fields["campaign"] is not None else V1_FIELDS
    record = json.dumps({key: fields[key] for key in keys}).encode()
    try:
        decode(record)
        accepted = True
    except InvalidEvent:
        accepted = False
    kafka = {k: v for k, v in raw(0).items() if k not in FIELDS}
    row = {**fields, **kafka, "event_date": _event_date(fields["event_time"])}
    columns = COLUMNS["raw_events"]
    con = empty_run
    con.execute("DELETE FROM raw_events")
    con.execute(f"INSERT INTO raw_events VALUES ({', '.join('?' for _ in columns)})", [row[c] for c in columns])
    report = run_rules(con)
    assert report.ok == accepted, (record, failing(report.lines()))
