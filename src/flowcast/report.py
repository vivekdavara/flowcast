"""A static HTML report of one run (DESIGN.md "The report (day 5)").

`flowcast report --out RUN --html FILE` writes one self-contained page: no scripts, and nothing loaded from anywhere
else, so it opens from disk, can be attached to a release, and shows the same thing in a year. It reads what a reader
of the outputs sees (the DuckDB views, the sink logs, the saved progress reports), as `quality` and `latency` do, and
it computes every number with the same functions those commands print, so the page and the terminal can't disagree.
With `--topic` it also runs the exact check, which needs the broker; with `--bench` it adds benchmark results.

`collect` gathers the numbers and `render` turns them into HTML; `render` is a pure function of `RunReport`, so the
page for given numbers is always the same bytes.
"""

from __future__ import annotations

import html
import json
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flowcast import __version__
from flowcast.check import Report
from flowcast.latency import WINDOW_MS, Latencies, Summary, Throughput, latencies, throughput
from flowcast.layout import Paths, current_backfill
from flowcast.quality import QualityReport, run_rules
from flowcast.restarts import DUCKDB_QUERY, FILE_SINK_QUERIES
from flowcast.views import SinkLogError, connect

# Up to a day of minutes is drawn as a continuous axis (minutes with no events as gaps); a longer span (a stray
# event decades away is valid input) draws only the minutes that have rows, and says so.
MAX_CONTINUOUS_MINUTES = 1_440


@dataclass(frozen=True)
class Minute:
    """One minute of event time: the stream's rows summed over event types, and the backfill's events."""

    start_ms: int
    events: int
    revenue_cents: int
    backfill_events: int | None  # None when the run has no published backfill


@dataclass(frozen=True)
class RunReport:
    root: str
    generated: str  # when the page was made, as given (the page shows it; tests fix it)
    rows: dict[str, int]  # per view
    minutes: tuple[Minute, ...]  # in time order, only minutes with rows
    types: tuple[tuple[str, int, int], ...]  # (event type, events, revenue cents) from minute_aggregates
    has_backfill: bool
    quality: QualityReport
    latency: Latencies | str  # the latencies, or why they couldn't be measured
    throughput: tuple[Throughput, ...]
    check: tuple[tuple[str, ...], bool] | None = None  # the exact check's lines and verdict, if it was run
    bench: tuple[dict[str, Any], ...] = ()  # entries of a bench.py results.json


def collect(
    root: Path,
    generated: str,
    *,
    check: Report | None = None,
    bench: Path | None = None,
) -> RunReport:
    """Everything the page shows about the run under `root`."""
    has_backfill = current_backfill(Paths(root)) is not None
    con = connect(root)
    try:
        quality = run_rules(con)
        minutes = con.execute(
            # A row with a NULL key or value (the rules report it) adds nothing: sum skips NULLs, and a window with
            # no start has no place on the axis.
            """WITH s AS (SELECT window_start, sum(events) AS events, sum(revenue_cents) AS revenue
                          FROM minute_aggregates WHERE window_start IS NOT NULL GROUP BY window_start),
                    b AS (SELECT window_start, sum(events) AS events FROM backfill_aggregates
                          WHERE window_start IS NOT NULL GROUP BY window_start)
               SELECT window_start, coalesce(s.events, 0), coalesce(s.revenue, 0), b.events
               FROM s FULL JOIN b USING (window_start) ORDER BY window_start"""
        ).fetchall()
        types = con.execute(
            "SELECT coalesce(event_type, 'NULL'), coalesce(sum(events), 0), coalesce(sum(revenue_cents), 0) "
            "FROM minute_aggregates GROUP BY event_type ORDER BY 2 DESC, 1"
        ).fetchall()
    finally:
        con.close()
    measured: Latencies | str
    try:
        measured = latencies(root)
    except SinkLogError as exc:
        measured = str(exc)
    rates = (throughput(root, query) for query in (*FILE_SINK_QUERIES, DUCKDB_QUERY))
    return RunReport(
        root=str(root),
        generated=generated,
        rows=quality.rows,
        minutes=tuple(
            Minute(int(start), int(events), int(revenue), None if not has_backfill else int(backfill or 0))
            for start, events, revenue, backfill in minutes
        ),
        types=tuple((str(t), int(e), int(r)) for t, e, r in types),
        has_backfill=has_backfill,
        quality=quality,
        latency=measured,
        throughput=tuple(rate for rate in rates if rate is not None),
        check=(tuple(check.lines()), check.ok) if check is not None else None,
        bench=load_bench(bench) if bench is not None else (),
    )


class BenchError(Exception):
    """A results file that isn't what scripts/bench.py writes."""


# The keys each benchmark's rows must have, all numbers (bools for check_ok); the latency summaries are optional.
_BENCH_KEYS = {
    "sizes": (
        "size",
        "raw_events_batches",
        "wall_s",
        "records_per_wall_s",
        "dropped_late",
        "dropped_late_pct",
        "predicted_dropped",
    ),
    "triggers": ("trigger_s", "records", "raw_events_batches", "slept_s"),
}
_SUMMARY_KEYS = ("count", "p50", "p95", "p99", "max")


# Far above any count, millisecond or second a benchmark here records, and far below where a chart's arithmetic
# overflows (a p99 of 1.7e308 did: day-5 review).
MAX_BENCH_NUMBER = 1e15


def _number(value: object) -> bool:
    """An int or float of at most MAX_BENCH_NUMBER either way; not a bool, NaN or infinity."""
    if not isinstance(value, int | float) or isinstance(value, bool):
        return False
    try:
        return abs(float(value)) <= MAX_BENCH_NUMBER  # False for NaN
    except OverflowError:  # an int past 1.8e308
        return False


def load_bench(path: Path) -> tuple[dict[str, Any], ...]:
    """`scripts/bench.py`'s results.json, checked, so the page can rely on it: a list of runs, each with its kind and
    rows. Runs of a kind the page doesn't know are skipped."""
    try:
        data = json.loads(path.read_text())
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:  # ValueError: JSONDecodeError, or 4,300+ digits
        raise BenchError(f"{path}: not JSON ({exc})") from exc
    if not isinstance(data, list):
        raise BenchError(f"{path}: not a list of benchmark runs")
    runs = []
    for number, run in enumerate(data):
        if not isinstance(run, dict) or not isinstance(run.get("rows"), list):
            raise BenchError(f"{path}: run {number} has no list of rows")
        keys = _BENCH_KEYS.get(str(run.get("bench")))
        if keys is None:
            continue
        for row in run["rows"]:
            if not isinstance(row, dict) or not all(_number(row.get(k)) for k in keys):
                raise BenchError(
                    f"{path}: run {number} ({run['bench']}): a row without {', '.join(keys)} as numbers "
                    f"(at most {MAX_BENCH_NUMBER:g})"
                )
            if run["bench"] == "sizes" and not isinstance(row.get("check_ok"), bool):
                raise BenchError(f"{path}: run {number} (sizes): a row without check_ok")
            for name in ("produced_ms", "event_ms", "window_ms"):
                summary = row.get(name)
                if summary is not None and not (
                    isinstance(summary, dict) and all(_number(summary.get(k)) for k in _SUMMARY_KEYS)
                ):
                    raise BenchError(f"{path}: run {number} ({run['bench']}): {name} isn't a latency summary")
        runs.append(run)
    return tuple(runs)


# ---------------------------------------------------------------------------------------------------------------
# HTML

_e = html.escape


def _n(value: float) -> str:
    # An int exactly (a float past 2**53 isn't); a float rounded.
    return f"{value:,}" if isinstance(value, int) else f"{value:,.0f}"


def _dollars(cents: int) -> str:
    """Integer cents as dollars, exactly: cents / 100 in floats is wrong past 2**53 cents."""
    dollars, rest = divmod(abs(cents), 100)
    return f"{'-' if cents < 0 else ''}${dollars:,}.{rest:02d}"


def _clock(ms: int, *, date: bool = True) -> str:
    """The window's start as a UTC date and time (only the time with `date=False`), with seconds and milliseconds
    when it isn't on a minute; a time no calendar holds as its number. The rules report both kinds of window."""
    try:
        moment = datetime.fromtimestamp(ms // 1000, UTC).replace(microsecond=ms % 1000 * 1000)
    except (OverflowError, OSError, ValueError):
        return f"{ms:,} ms"
    text = moment.strftime("%Y-%m-%d %H:%M" if date else "%H:%M")
    return text if ms % WINDOW_MS == 0 else f"{text}:{moment.second:02d}.{ms % 1000:03d}"


def _money(dollars: float) -> str:
    return f"${dollars:,.0f}" if dollars == int(dollars) else f"${dollars:,.2f}"


def nice_ticks(top: float, count: int = 4, *, integer: bool = False) -> list[float]:
    """Axis ticks from 0 to at least `top` in steps of 1, 2 or 5 times a power of ten (at least 1 for `integer`
    quantities such as counts, which a tick at 0.5 would mislabel)."""
    if not top > 1e-9:  # nothing to show (or less than a float can step through): a unit axis
        return [0.0, 1.0]
    raw = top / count
    power = 10 ** math.floor(math.log10(raw))
    step = next(m * power for m in (1, 2, 5, 10) if m * power >= raw)
    if integer:
        step = max(step, 1)
    return [i * step for i in range(max(1, math.ceil(top / step - 1e-9)) + 1)]


@dataclass(frozen=True)
class Series:
    name: str
    css: str  # the class that colours its bars (and its legend swatch)
    values: tuple[float, ...]


def bar_chart(
    title: str,
    labels: Sequence[str],
    series: Sequence[Series],
    *,
    stacked: bool,
    unit: Callable[[float], str] = _n,
    integer: bool = True,
    tick_every: int | None = None,
) -> str:
    """An SVG bar chart: one bar per label, the series stacked or side by side. Each bar has a <title> (the browser
    shows it on hover); the table under the chart holds the same numbers."""
    width, height = 720, 240
    left, right, top, bottom = 64, 8, 10, 30
    plot_w, plot_h = width - left - right, height - top - bottom
    n = len(labels)
    # Only positive values are drawn (the tables show the rest), so only they size the axis.
    drawn = [[max(s.values[i], 0.0) for s in series] for i in range(n)]
    totals = [sum(values) if stacked else max(values) for values in drawn]
    ticks = nice_ticks(max(totals, default=0.0), integer=integer)
    scale = plot_h / ticks[-1]
    out = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{_e(title)}" class="chart">',
    ]
    for tick in ticks:
        y = top + plot_h - tick * scale
        out.append(f'<line class="grid" x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}"/>')
        out.append(f'<text class="tick" x="{left - 6}" y="{y + 4:.1f}" text-anchor="end">{_e(unit(tick))}</text>')
    slot = plot_w / max(n, 1)
    per_group = 1 if stacked else len(series)
    gap = 2 if slot / per_group >= 4 else 0
    bar_w = max((slot - gap) / per_group - (gap if per_group > 1 else 0), 0.5)
    for i, label in enumerate(labels):
        x0 = left + i * slot + gap / 2
        base = 0.0
        tip = _e(f"{label}: " + ", ".join(f"{s.name} {unit(s.values[i])}" for s in series))
        for k, s in enumerate(series):
            value = s.values[i]
            if value <= 0:
                continue
            h = value * scale
            y = top + plot_h - (base + value) * scale
            x = x0 if stacked else x0 + k * (bar_w + gap)
            out.append(
                f'<rect class="{s.css}" x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}">'
                f"<title>{tip}</title></rect>"
            )
            if stacked:
                base += value
    every = tick_every or max(1, math.ceil(n / 6))
    for i in range(0, n, every):
        x = left + (i + 0.5) * slot
        out.append(f'<text class="tick" x="{x:.1f}" y="{height - 10}" text-anchor="middle">{_e(labels[i])}</text>')
    out.append(f'<line class="axis" x1="{left}" x2="{width - right}" y1="{top + plot_h}" y2="{top + plot_h}"/>')
    out.append("</svg>")
    legend = ""
    if len(series) > 1:
        legend = (
            '<p class="legend">' + "".join(f'<span><i class="{s.css}"></i>{_e(s.name)}</span>' for s in series) + "</p>"
        )
    return legend + "".join(out)


def table(header: Sequence[str], rows: Sequence[Sequence[object]], *, numeric_from: int = 1) -> str:
    """An HTML table; columns from `numeric_from` on are right-aligned figures."""
    head = "".join(f"<th{' class=num' if j >= numeric_from else ''}>{_e(h)}</th>" for j, h in enumerate(header))
    body = "".join(
        "<tr>"
        + "".join(f"<td{' class=num' if j >= numeric_from else ''}>{_e(str(c))}</td>" for j, c in enumerate(row))
        + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _status(ok: bool, good: str = "ok", bad: str = "FAIL") -> str:
    # Never colour alone: an icon and a word with it.
    return f'<span class="status good">✓ {good}</span>' if ok else f'<span class="status bad">✕ {bad}</span>'


def _axis_minutes(minutes: Sequence[Minute]) -> tuple[list[Minute], bool]:
    """The minutes to draw: every minute from the first to the last (zeros where there are no rows) when that is at
    most a day and every window starts on a minute, else only those with rows (a window off the minute grid would
    fall between the drawn minutes and vanish: day-5 review). The flag says whether the axis is continuous."""
    if not minutes:
        return [], True
    first, last = minutes[0].start_ms, minutes[-1].start_ms
    span = (last - first) // WINDOW_MS + 1
    if span > MAX_CONTINUOUS_MINUTES or any(m.start_ms % WINDOW_MS for m in minutes):
        return list(minutes), False
    by_start = {m.start_ms: m for m in minutes}
    empty_backfill = None if minutes[0].backfill_events is None else 0
    return [
        by_start.get(first + i * WINDOW_MS, Minute(first + i * WINDOW_MS, 0, 0, empty_backfill)) for i in range(span)
    ], True


def _events_section(r: RunReport) -> str:
    drawn, continuous = _axis_minutes(r.minutes)
    if not drawn:
        return "<h2>Events per minute of event time</h2><p>No windows written yet.</p>"
    # Within a day the time of day names a minute; otherwise each label needs its date.
    labels = [_clock(m.start_ms, date=not continuous) for m in drawn]
    note = (
        ""
        if continuous
        else "<p class=note>The windows span over a day or don't all start on a minute, so only minutes with rows are "
        "drawn.</p>"
    )
    if r.has_backfill:
        stream = tuple(float(m.events) for m in drawn)
        late = tuple(float(max((m.backfill_events or 0) - m.events, 0)) for m in drawn)
        series = [Series("counted by the stream", "s1", stream), Series("only in the backfill (late)", "s2", late)]
        dropped = int(sum(late))
        below = sum(1 for m in drawn if (m.backfill_events or 0) < m.events)
        intro = (
            f"<p>Each bar is a minute's events in the backfill (the truth of every raw event); the top part is what "
            f"the stream dropped as late: {_n(dropped)} events in all.</p>"
        )
        if below:
            # A correct run can't have it (`flowcast check` fails); the table shows the difference (day-5 review).
            intro += (
                f"<p class=note>In {_n(below)} minute{'' if below == 1 else 's'} the backfill has fewer events than "
                "the stream, which a correct run can't have; those bars show the stream's count, and the table the "
                "difference.</p>"
            )
        rows: list[tuple[object, ...]] = [
            (_clock(m.start_ms), m.events, m.backfill_events, (m.backfill_events or 0) - m.events) for m in drawn
        ]
        header = ["minute (UTC)", "stream", "backfill", "late"]
    else:
        series = [Series("events", "s1", tuple(float(m.events) for m in drawn))]
        intro = "<p>The stream's minute windows, all event types together.</p>"
        rows = [(_clock(m.start_ms), m.events) for m in drawn]
        header = ["minute (UTC)", "events"]
    chart = bar_chart("Events per minute of event time", labels, series, stacked=True)
    revenue = bar_chart(
        "Revenue per minute",
        labels,
        [Series("revenue", "s1", tuple(m.revenue_cents / 100 for m in drawn))],
        stacked=True,
        unit=_money,
        integer=False,
    )
    revenue_rows = [(_clock(m.start_ms), _dollars(m.revenue_cents)) for m in drawn]
    return (
        f"<h2>Events per minute of event time</h2>{intro}{note}{chart}"
        f"<details><summary>Table</summary>{table(header, rows)}</details>"
        f"<h2>Revenue per minute</h2><p>Purchases only, from the stream's windows.</p>{revenue}"
        f"<details><summary>Table</summary>{table(['minute (UTC)', 'revenue'], revenue_rows)}</details>"
    )


def _summary_row(name: str, s: Summary | None) -> list[object]:
    if s is None:
        return [name, 0, "", "", "", ""]
    return [name, _n(s.count), _n(s.p50), _n(s.p95), _n(s.p99), _n(s.max)]


def _latency_section(r: RunReport) -> str:
    out = ["<h2>Latency and throughput</h2>"]
    if isinstance(r.latency, str):
        out.append(f"<p>Not measured: {_e(r.latency)}</p>")
    else:
        lat = r.latency
        out.append(
            "<p>From each row's own moment to the commit of the batch that made it queryable (its sink log entry), in "
            f"milliseconds; {lat.batches} raw_events batches. Event time is meaningful only for a run fed in real "
            "time (<code>produce --speed 1 --start-ms now</code>).</p>"
        )
        rows = [
            _summary_row("produced to queryable (raw_events)", lat.produced),
            _summary_row("event time to queryable (raw_events)", lat.event),
            _summary_row("window end to queryable (minute_aggregates)", lat.window),
        ]
        out.append(table(["latency", "rows", "p50", "p95", "p99", "max"], rows))
        if lat.produced is not None and lat.event is not None:
            behind = (lat.event.p50 - lat.produced.p50) / 86_400_000
            if behind > 1:
                out.append(
                    f"<p class=note>This run's events were produced {behind:,.1f} days after their event times (a "
                    "backlog, not a live stream), so the event-time and window-end rows measure that, not the "
                    "pipeline.</p>"
                )
    if r.throughput:
        rows = [
            [t.query, _n(t.rows), t.batches, f"{t.span_ms / 1000:,.1f}", _n(t.rows_per_s), _n(t.rows_per_busy_s)]
            for t in r.throughput
        ]
        header = ["query", "rows", "batches", "span (s)", "rows/s", "rows per busy s"]
        out.append(
            "<p>Each query's reading rate, from Spark's progress reports, saved when a run of the job ends: after "
            "restarts they cover the rounds that ended, not those killed.</p>"
        )
        out.append(table(header, rows))
    else:
        out.append("<p>No progress reports saved (a killed run saves none).</p>")
    return "".join(out)


def _quality_section(q: QualityReport) -> str:
    rows = [
        [
            r.view,
            r.name,
            _n(r.failures),
            "; ".join(" ".join(map(str, s)) for s in r.samples),
        ]
        for r in q.results
    ]
    statuses = [_status(r.ok) for r in q.results]
    body = "".join(
        f"<tr><td>{status}</td>"
        + "".join(f"<td{' class=num' if j == 2 else ''}>{_e(str(c))}</td>" for j, c in enumerate(row))
        + "</tr>"
        for status, row in zip(statuses, rows, strict=True)
    )
    over = q.dead_letter_rate > q.max_dead_letter_rate
    rate = (
        f"<tr><td>{_status(not over)}</td><td>dead_letters</td><td>at most {q.max_dead_letter_rate:.2%} of records"
        f"</td><td class=num>{q.dead_letter_rate:.4%}</td><td></td></tr>"
    )
    head = "<tr><th></th><th>view</th><th>rule</th><th class=num>failing rows</th><th>e.g.</th></tr>"
    return (
        f"<h2>Data quality {_status(q.ok, 'ok', 'FAILED')}</h2>"
        "<p>Each rule is SQL over the views, counting the rows that break it (<code>flowcast quality</code>).</p>"
        f"<table><thead>{head}</thead><tbody>{body}{rate}</tbody></table>"
    )


def _check_section(check: tuple[tuple[str, ...], bool] | None) -> str:
    if check is None:
        return (
            "<h2>Exact check</h2><p>Not run for this page: <code>flowcast report --topic TOPIC</code> runs it "
            "(it replays the job's batches over the Kafka records, so it needs the broker).</p>"
        )
    lines, ok = check
    return (
        f"<h2>Exact check {_status(ok, 'ok', 'MISMATCH')}</h2>"
        "<p>The job's own batches replayed over the topic's records: every output must equal the replay.</p>"
        f"<pre>{_e(chr(10).join(lines))}</pre>"
    )


def _ms(summary: object, key: str) -> str:
    return _n(summary[key]) if isinstance(summary, dict) else ""


def _bench_section(bench: Sequence[dict[str, Any]]) -> str:
    if not bench:
        return ""
    out = ["<h2>Benchmarks</h2><p>From <code>scripts/bench.py</code>'s results.json, each run as recorded.</p>"]
    for entry in bench:
        rows = entry.get("rows", [])
        at = _e(str(entry.get("at", "")))
        if entry.get("bench") == "sizes":
            out.append(f"<h3>Batch size over a backlog ({at})</h3>")
            body = [
                [
                    _n(row["size"]),
                    row["raw_events_batches"],
                    f"{row['wall_s']:,.1f}",
                    _n(row["records_per_wall_s"]),
                    f"{_n(row['dropped_late'])} ({row['dropped_late_pct']:g}%)",
                    _n(row["predicted_dropped"]),
                    "ok" if row["check_ok"] else "MISMATCH",
                ]
                for row in rows
            ]
            header = [
                "records a batch",
                "batches with rows",
                "wall (s)",
                "records/s",
                "dropped late",
                "predicted",
                "check",
            ]
            out.append(table(header, body, numeric_from=0))
        elif entry.get("bench") == "triggers":
            out.append(f"<h3>Trigger interval on a live stream ({at})</h3>")
            body = [
                [
                    f"{row['trigger_s']:g} s",
                    _n(row["records"]),
                    row["raw_events_batches"],
                    f"{row['slept_s'] + 0.0:g}",  # + 0.0: a sleep of -0.0 s reads 0
                    *(_ms(row.get("produced_ms"), p) for p in ("p50", "p95", "p99")),
                    *(_ms(row.get("event_ms"), p) for p in ("p50", "p99")),
                    _ms(row.get("window_ms"), "p50"),
                ]
                for row in rows
            ]
            header = [
                "trigger",
                "records",
                "batches with rows",
                "slept (s)",
                "produced p50",
                "p95",
                "p99",
                "event time p50",
                "p99",
                "window end p50",
            ]
            out.append(table(header, body))
            if rows and all(isinstance(row.get("produced_ms"), dict) for row in rows):
                series = [
                    Series(f"produced to queryable {p}", f"o{k}", tuple(float(row["produced_ms"][p]) for row in rows))
                    for k, p in enumerate(("p50", "p95", "p99"), start=1)
                ]
                labels = [f"trigger {row['trigger_s']:g} s" for row in rows]
                out.append(
                    bar_chart(
                        "Produced to queryable by trigger",
                        labels,
                        series,
                        stacked=False,
                        unit=lambda v: f"{v:,.0f} ms",
                        tick_every=1,
                    )
                )
    return "".join(out)


_CSS = """
:root{color-scheme:light;--surface:#fcfcfb;--page:#f9f9f7;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;
--grid:#e1e0d9;--axis:#c3c2b7;--border:rgba(11,11,11,.10);--s1:#2a78d6;--s2:#eb6834;
--o1:#86b6ef;--o2:#2a78d6;--o3:#104281;--good:#006300;--bad:#d03b3b}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--surface:#1a1a19;
--page:#0d0d0d;--ink:#f0efec;--ink2:#c3c2b7;--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);
--s1:#3987e5;--s2:#d95926;--o1:#256abf;--o2:#5598e7;--o3:#9ec5f4;--good:#0ca30c;--bad:#e66767}}
:root[data-theme="dark"]{color-scheme:dark;--surface:#1a1a19;--page:#0d0d0d;--ink:#f0efec;--ink2:#c3c2b7;
--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);--s1:#3987e5;--s2:#d95926;--o1:#256abf;
--o2:#5598e7;--o3:#9ec5f4;--good:#0ca30c;--bad:#e66767}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:960px;margin:0 auto;padding:24px 16px 48px}
h1{font-size:26px;margin:0 0 4px}h2{font-size:19px;margin:32px 0 8px}h3{font-size:16px;margin:20px 0 6px}
p{margin:6px 0;color:var(--ink2)}.meta{font-size:13px}.note{font-size:13px}
code,pre{font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace}
pre{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;overflow-x:auto}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:16px 0}
.tile{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px}
.tile b{display:block;font-size:22px}.tile span{font-size:13px;color:var(--ink2)}
.chart{width:100%;height:auto;background:var(--surface);border:1px solid var(--border);border-radius:10px}
.chart .grid{stroke:var(--grid);stroke-width:1}.chart .axis{stroke:var(--axis);stroke-width:1}
.chart .tick{fill:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}
.chart rect{stroke:var(--surface);stroke-width:1}
.s1{fill:var(--s1);background:var(--s1)}.s2{fill:var(--s2);background:var(--s2)}
.o1{fill:var(--o1);background:var(--o1)}.o2{fill:var(--o2);background:var(--o2)}
.o3{fill:var(--o3);background:var(--o3)}
.legend{display:flex;flex-wrap:wrap;gap:16px;font-size:13px;color:var(--ink2)}
.legend i{display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:6px;vertical-align:-1px}
.table-wrap{overflow-x:auto}
table{border-collapse:collapse;width:100%;margin:8px 0;font-size:13px;background:var(--surface)}
th,td{border-bottom:1px solid var(--grid);padding:5px 8px;text-align:left;vertical-align:top}
th{color:var(--ink2);font-weight:600}.num{text-align:right;font-variant-numeric:tabular-nums}
td{overflow-wrap:anywhere}
details{margin:6px 0}summary{cursor:pointer;color:var(--ink2);font-size:13px}
.status{font-weight:600;font-size:13px;white-space:nowrap}.good{color:var(--good)}.bad{color:var(--bad)}
"""


def render(r: RunReport) -> str:
    """The page for the run's numbers."""
    first = r.minutes[0].start_ms if r.minutes else None
    last = r.minutes[-1].start_ms if r.minutes else None
    span = "none" if first is None or last is None else f"{_clock(first)} to {_clock(last + WINDOW_MS)} UTC"
    revenue = sum(m.revenue_cents for m in r.minutes)
    tiles = [
        (_n(r.rows.get("raw_events", 0)), "raw events"),
        (_n(r.rows.get("minute_aggregates", 0)), "window rows"),
        (_n(r.rows.get("dead_letters", 0)), "dead letters"),
        (_n(sum(1 for m in r.minutes if m.events)), "minutes in the windows"),
        (_dollars(revenue), "revenue in windows"),
    ]
    tiles_html = "".join(f'<div class="tile"><b>{_e(v)}</b><span>{_e(k)}</span></div>' for v, k in tiles)
    type_rows = [(t, _n(e), _dollars(rev)) for t, e, rev in r.types]
    verdicts = [f"data quality {_status(r.quality.ok, 'ok', 'FAILED')}"]
    if r.check is not None:
        verdicts.append(f"exact check {_status(r.check[1], 'ok', 'MISMATCH')}")
    body = "".join(
        [
            "<h1>FlowCast run report</h1>",
            f'<p class="meta">Run <code>{_e(r.root)}</code> · generated {_e(r.generated)} · flowcast {__version__}'
            " · synthetic data: every event came from the seeded generator.</p>",
            f'<p class="meta">Event time {_e(span)}. {" · ".join(verdicts)}</p>',
            f'<div class="tiles">{tiles_html}</div>',
            _events_section(r),
            "<h2>By event type</h2>",
            table(["event type", "events", "revenue"], type_rows),
            _latency_section(r),
            _quality_section(r.quality),
            _check_section(r.check),
            _bench_section(r.bench),
        ]
    )
    # Wide tables scroll inside their own box at phone width rather than widening the page.
    body = body.replace("<table>", '<div class="table-wrap"><table>').replace("</table>", "</table></div>")
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>FlowCast run report</title><style>{_CSS}</style></head><body><main>{body}</main></body></html>\n"
    )


def write(path: Path, report: RunReport) -> None:
    """Write the page next to its final name, then rename it into place: a reader never sees half a page."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(render(report), encoding="utf-8")
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
