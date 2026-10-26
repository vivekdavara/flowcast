"""Data-quality rules over a run's outputs, as SQL on the DuckDB views (DESIGN.md "Data quality (day 4)").

`check` (check.py) holds a stopped run to the exact replay of its Kafka records; it needs the broker and the
checkpoints. These rules need neither: each is a query over what a reader of the outputs sees, so they also run on a
copy of the Parquet files, or while the job is still writing. A rule counts the rows that break it; a run passes when
every count is 0. They restate what the decoder and the job promise (every raw event is one the decoder would accept,
a window is written once, ...), so a failure means a bug upstream or files that something else changed.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from flowcast.schema import (
    DEVICES,
    EVENT_TYPES,
    MAX_EVENT_TIME,
    MAX_PRICE_CENTS,
    MAX_QUANTITY,
    MIN_EVENT_TIME,
    PRODUCT_TYPES,
    QUANTITY_TYPES,
    SCHEMA_VERSIONS,
)
from flowcast.views import connect

# More dead letters than this share of all records fails the run: a producer sending bad records in bulk is an
# incident even though each one is handled. The generator's invalid records are far below it (3 in 11,969 on day 3).
MAX_DEAD_LETTER_RATE = 0.01
SAMPLES = 3


def _in(values: tuple[object, ...] | frozenset[str]) -> str:
    return "(" + ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in sorted(values, key=str)) + ")"


_RECORD = ("topic", "partition", "offset")
_WINDOW = ("window_start", "event_type")
_REQUIRED = (
    "schema_version",
    "event_id",
    "event_type",
    "event_time",
    "user_id",
    "session_id",
    "device",
    "country",
    "page",
    *_RECORD,
    "kafka_timestamp",
    "event_date",
)
_PRODUCT = f"coalesce(event_type IN {_in(PRODUCT_TYPES)}, false)"
_QUANTITY = f"coalesce(event_type IN {_in(QUANTITY_TYPES)}, false)"


@dataclass(frozen=True)
class Rule:
    """A data-quality rule: `bad` is a WHERE clause over `view` that selects the rows that break it, and `key` the
    columns that name a row in the report."""

    name: str
    view: str
    bad: str
    key: tuple[str, ...]


# Row by row: each row of the view must pass on its own.
ROW_RULES = (
    Rule("required fields set", "raw_events", " OR ".join(f'"{c}" IS NULL' for c in _REQUIRED), _RECORD),
    Rule("known event type", "raw_events", f"event_type NOT IN {_in(EVENT_TYPES)}", _RECORD),
    Rule("known device", "raw_events", f"device NOT IN {_in(DEVICES)}", _RECORD),
    Rule(
        "known schema version, campaign only in version 2",
        "raw_events",
        f"schema_version NOT IN {_in(SCHEMA_VERSIONS)} OR (schema_version = 1 AND campaign IS NOT NULL)",
        _RECORD,
    ),
    Rule(
        "event time in range", "raw_events", f"event_time < {MIN_EVENT_TIME} OR event_time >= {MAX_EVENT_TIME}", _RECORD
    ),
    Rule(
        "product fields match the event type",
        "raw_events",
        " OR ".join(f"({c} IS NOT NULL) <> {_PRODUCT}" for c in ("product_id", "category", "price_cents"))
        + f" OR (quantity IS NOT NULL) <> {_QUANTITY}",
        _RECORD,
    ),
    Rule(
        "price and quantity in range",
        "raw_events",
        f"price_cents NOT BETWEEN 1 AND {MAX_PRICE_CENTS} OR quantity NOT BETWEEN 1 AND {MAX_QUANTITY}",
        _RECORD,
    ),
    Rule(
        "event date is the event time's UTC date",
        "raw_events",
        # Only for times in range (the rule above reports the rest): event_time * 1000 can overflow (day-4 review).
        f"CASE WHEN event_time >= {MIN_EVENT_TIME} AND event_time < {MAX_EVENT_TIME} "
        "THEN event_date <> CAST(make_timestamp(event_time * 1000) AS DATE) END",
        _RECORD,
    ),
    # A NULL key would pass every other window rule (a comparison with NULL is NULL, so the row isn't counted).
    Rule("required fields set", "minute_aggregates", "window_start IS NULL OR event_type IS NULL", _WINDOW),
    Rule("known event type", "minute_aggregates", f"event_type NOT IN {_in(EVENT_TYPES)}", _WINDOW),
    Rule("window starts on a minute", "minute_aggregates", "window_start % 60000 <> 0", _WINDOW),
    Rule(
        "counts positive, revenue only from purchases",
        "minute_aggregates",
        "events IS NULL OR events < 1 OR revenue_cents IS NULL OR revenue_cents < 0 "
        "OR (event_type <> 'purchase' AND revenue_cents <> 0)",
        _WINDOW,
    ),
    Rule("dead letters have a reason", "dead_letters", "error IS NULL OR error = ''", _RECORD),
    Rule("dead letters have their record", "dead_letters", " OR ".join(f'"{c}" IS NULL' for c in _RECORD), _RECORD),
)

# Across rows: each query returns the offending rows (the key columns first).
SET_RULES = (
    (
        "each Kafka record once, as an event or a dead letter",
        "raw_events + dead_letters",
        """SELECT topic, "partition", "offset", count(*) AS copies
           FROM (SELECT topic, "partition", "offset" FROM raw_events
                 UNION ALL SELECT topic, "partition", "offset" FROM dead_letters)
           GROUP BY ALL HAVING count(*) > 1""",
    ),
    (
        "each window and event type once",
        "minute_aggregates",
        "SELECT window_start, event_type, count(*) AS copies FROM minute_aggregates GROUP BY ALL HAVING count(*) > 1",
    ),
    # The stream drops late records, so a window can count fewer events than the raw output holds, never more. Raw
    # events are deduplicated by event_id (a re-sent copy carries the same revenue). On a run that is still writing,
    # raw_events can trail minute_aggregates by a batch, so a failure there is only conclusive on a stopped run.
    (
        "no window counts more than the raw events",
        "minute_aggregates vs raw_events",
        """WITH events AS (
               SELECT event_id, any_value(event_time // 60000 * 60000) AS window_start,
                      any_value(event_type) AS event_type,
                      max(CASE WHEN event_type = 'purchase' THEN price_cents::HUGEINT * quantity ELSE 0 END) AS rev
               FROM raw_events GROUP BY event_id),
           raw AS (SELECT window_start, event_type, count(*) AS events, sum(rev) AS revenue_cents
                   FROM events GROUP BY ALL)
           SELECT a.window_start, a.event_type, a.events, coalesce(r.events, 0) AS raw_events
           FROM minute_aggregates a LEFT JOIN raw r USING (window_start, event_type)
           WHERE r.events IS NULL OR a.events > r.events OR a.revenue_cents > r.revenue_cents""",
    ),
)


@dataclass(frozen=True)
class Result:
    name: str
    view: str
    failures: int
    samples: tuple[tuple[object, ...], ...]

    @property
    def ok(self) -> bool:
        return self.failures == 0


@dataclass(frozen=True)
class QualityReport:
    rows: dict[str, int]  # per view
    results: tuple[Result, ...]
    dead_letter_rate: float
    max_dead_letter_rate: float

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.results) and self.dead_letter_rate <= self.max_dead_letter_rate

    def lines(self) -> list[str]:
        out = ["rows: " + ", ".join(f"{view} {n:,}" for view, n in self.rows.items())]
        for r in self.results:
            line = f"{'ok  ' if r.ok else 'FAIL'}  {r.view}: {r.name}"
            if not r.ok:
                line += f": {r.failures:,} rows, e.g. " + "; ".join(" ".join(map(str, s)) for s in r.samples)
            out.append(line)
        over = self.dead_letter_rate > self.max_dead_letter_rate
        out.append(
            f"{'FAIL' if over else 'ok  '}  dead_letters: {self.dead_letter_rate:.4%} of records "
            f"(at most {self.max_dead_letter_rate:.2%})"
        )
        out.append("quality: " + ("ok" if self.ok else "FAILED"))
        return out


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


def run_rules(con: duckdb.DuckDBPyConnection, max_dead_letter_rate: float = MAX_DEAD_LETTER_RATE) -> QualityReport:
    """Every rule over the views of `con` (views.connect)."""
    if not 0 <= max_dead_letter_rate <= 1:
        raise ValueError("max_dead_letter_rate must be between 0 and 1")
    rows = {
        view: _count(con, f"SELECT count(*) FROM {view}")
        for view in ("raw_events", "minute_aggregates", "dead_letters")
    }
    results = []
    for rule in ROW_RULES:
        # A row for which `bad` is NULL (a null in a comparison) isn't counted; where null is the fault, a rule says
        # IS NULL.
        where = f"FROM {rule.view} WHERE {rule.bad}"
        key = ", ".join(f'"{c}"' for c in rule.key)
        samples = con.execute(f"SELECT {key} {where} ORDER BY {key} LIMIT {SAMPLES}").fetchall()
        results.append(Result(rule.name, rule.view, _count(con, f"SELECT count(*) {where}"), tuple(samples)))
    for name, view, sql in SET_RULES:
        failures = _count(con, f"SELECT count(*) FROM ({sql})")
        samples = con.execute(f"SELECT * FROM ({sql}) ORDER BY ALL LIMIT {SAMPLES}").fetchall()
        results.append(Result(name, view, failures, tuple(samples)))
    records = rows["raw_events"] + rows["dead_letters"]
    rate = rows["dead_letters"] / records if records else 0.0
    return QualityReport(rows, tuple(results), rate, max_dead_letter_rate)


def check_quality(root: Path, max_dead_letter_rate: float = MAX_DEAD_LETTER_RATE) -> QualityReport:
    """The rules over the committed outputs of the run under `root`."""
    con = connect(root)
    try:
        return run_rules(con, max_dead_letter_rate)
    finally:
        con.close()
