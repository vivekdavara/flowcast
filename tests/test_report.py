"""report.py: the numbers the page shows, the page itself (escaped, self-contained, the same bytes for the same
numbers), and the command."""

import json
import re
from pathlib import Path

import duckdb
import pytest

from flowcast.cli import main
from flowcast.layout import Paths
from flowcast.report import (
    MAX_CONTINUOUS_MINUTES,
    Minute,
    Series,
    _axis_minutes,
    bar_chart,
    collect,
    nice_ticks,
    render,
)
from tests.test_quality import T0, aggregate, clean, parquet, raw, tables

GENERATED = "2026-10-27 19:00 UTC"


def run_dir(root: Path, data: dict[str, list[dict[str, object]]]) -> Path:
    """A run directory whose three sinks hold `data`, each as one committed batch."""
    con = duckdb.connect()
    try:
        tables(con, data)
        parquet(con, "raw_events", root / "raw_events", "0", "event_date=2026-01-01")
        parquet(con, "minute_aggregates", root / "minute_aggregates", "0")
        parquet(con, "dead_letters", root / "dead_letters", "0")
    finally:
        con.close()
    return root


def publish_backfill(root: Path, rows: list[dict[str, object]]) -> None:
    folder = Paths(root).backfill / "run-1"
    folder.mkdir(parents=True)
    con = duckdb.connect()
    try:
        tables(con, {"minute_aggregates": rows})
        con.execute(f"COPY minute_aggregates TO '{folder / 'part-0.parquet'}' (FORMAT parquet)")
    finally:
        con.close()
    (Paths(root).backfill / "CURRENT").write_text("run-1\n")


def test_nice_ticks() -> None:
    assert nice_ticks(0) == [0, 1]
    assert nice_ticks(175) == [0, 50, 100, 150, 200]
    assert nice_ticks(4) == [0, 1, 2, 3, 4]
    assert nice_ticks(1) == [0, 0.5, 1]
    # Counts never get a tick at 0.5, which the integer labels would print as 0.
    assert nice_ticks(1, integer=True) == [0, 1] and nice_ticks(2, integer=True) == [0, 1, 2]
    assert nice_ticks(0.1) == [0, 0.05, 0.1]
    assert nice_ticks(200) == [0, 50, 100, 150, 200]  # the top on a tick: no extra step
    for top in (0.3, 7, 99, 101, 12_345, 2.5e9):
        ticks = nice_ticks(top)
        assert ticks[0] == 0 and ticks[-1] >= top and len(ticks) <= 6, (top, ticks)


def test_collect_sums_the_windows_and_reads_the_rules(tmp_path: Path) -> None:
    root = run_dir(tmp_path, clean())
    r = collect(root, GENERATED)
    assert r.rows == {"raw_events": 103, "minute_aggregates": 3, "dead_letters": 1}
    assert r.minutes == (
        Minute(T0, 1, 1000, None),
        Minute(T0 + 60_000, 1, 0, None),
        Minute(T0 + 120_000, 100, 0, None),
    )
    assert r.types == (("page_view", 101, 0), ("purchase", 1, 1000))
    assert r.quality.ok and not r.has_backfill and r.check is None and r.bench == ()
    assert not isinstance(r.latency, str) and r.latency.produced is not None and r.latency.produced.count == 103
    assert r.throughput == ()  # no progress reports saved


def test_with_a_backfill_each_minute_carries_its_events(tmp_path: Path) -> None:
    root = run_dir(tmp_path, clean())
    # The backfill holds one more page view at T0 + 60 s (a late record the stream dropped) and a minute the stream
    # never wrote.
    publish_backfill(
        root,
        [
            aggregate(),
            aggregate(T0 + 60_000, "page_view", 2, 0),
            aggregate(T0 + 120_000, "page_view", 100, 0),
            aggregate(T0 + 240_000, "page_view", 3, 0),
        ],
    )
    r = collect(root, GENERATED)
    assert r.has_backfill
    assert r.minutes == (
        Minute(T0, 1, 1000, 1),
        Minute(T0 + 60_000, 1, 0, 2),
        Minute(T0 + 120_000, 100, 0, 100),
        Minute(T0 + 240_000, 0, 0, 3),
    )
    page = render(r)
    assert "what the stream dropped as late: 4 events in all" in page
    # The tile counts the stream's minutes, not the backfill's extra one (mutation pass).
    assert "<b>3</b><span>minutes in the windows</span>" in page
    assert "only in the backfill (late)" in page  # the legend: two series
    # The minute with no rows at all (T0 + 180 s) is drawn as a gap, and appears in the table as zeros.
    assert "<td>2026-01-01 00:03</td><td class=num>0</td><td class=num>0</td><td class=num>0</td>" in page


def test_event_types_are_listed_by_their_events(tmp_path: Path) -> None:
    """Most events first, ties by name: not alphabetical (mutation pass)."""
    data = clean()
    data["minute_aggregates"] += [aggregate(T0, "add_to_cart", 1, 0), aggregate(T0, "checkout", 500, 0)]
    r = collect(run_dir(tmp_path, data), GENERATED)
    assert [t for t, _, _ in r.types] == ["checkout", "page_view", "add_to_cart", "purchase"]


def test_the_page_shows_the_numbers(tmp_path: Path) -> None:
    page = render(collect(run_dir(tmp_path, clean()), GENERATED))
    assert page.startswith("<!doctype html>") and page.endswith("</html>\n")
    assert "<b>103</b><span>raw events</span>" in page
    assert "<b>3</b><span>minutes in the windows</span>" in page
    assert "<b>$10.00</b><span>revenue in windows</span>" in page
    assert "Event time 2026-01-01 00:00 to 2026-01-01 00:03 UTC" in page
    assert 'data quality <span class="status good">✓ ok</span>' in page
    assert "Not run for this page" in page  # the exact check
    assert "<td>purchase</td><td class=num>1</td><td class=num>$10.00</td>" in page
    # One bar per minute with events in each chart, each with a hover title; revenue only in the first minute.
    assert len(re.findall(r'<rect class="s1"', page)) == 3 + 1
    assert "<title>00:02: events 100</title>" in page


def test_the_page_is_self_contained_and_the_same_for_the_same_numbers(tmp_path: Path) -> None:
    r = collect(run_dir(tmp_path, clean()), GENERATED)
    page = render(r)
    for loads_something in ("<script", "src=", "href=", "url(", "@import", "://"):
        assert loads_something not in page
    assert render(r) == page
    assert render(collect(tmp_path, GENERATED)) == page


def test_values_from_the_run_are_escaped(tmp_path: Path) -> None:
    data = clean()
    # A record the rules report (an unknown device), named by its topic, which is the run's own data.
    data["raw_events"].append(raw(500, device="watch", topic="<b>x</b>&"))
    data["minute_aggregates"].append(aggregate(event_type="<i>", revenue=0))  # into the by-type table too
    root = run_dir(tmp_path / 'run <1> & "two"', data)
    page = render(collect(root, GENERATED))
    assert "<b>x</b>" not in page and "&lt;b&gt;x&lt;/b&gt;&amp; 0 500" in page
    assert "<i>" not in page and "<td>&lt;i&gt;</td>" in page
    assert "run &lt;1&gt; &amp; &quot;two&quot;" in page
    assert 'Data quality <span class="status bad">✕ FAILED</span>' in page


def test_an_empty_run(tmp_path: Path) -> None:
    r = collect(tmp_path, GENERATED)
    assert r.minutes == () and r.rows == {"raw_events": 0, "minute_aggregates": 0, "dead_letters": 0}
    page = render(r)
    assert "No windows written yet." in page and "Event time none." in page
    assert "<svg" not in page


def test_up_to_a_day_of_minutes_is_continuous() -> None:
    first = Minute(T0, 1, 0, None)
    for span, continuous in ((MAX_CONTINUOUS_MINUTES, True), (MAX_CONTINUOUS_MINUTES + 1, False)):
        last = Minute(T0 + (span - 1) * 60_000, 1, 0, None)
        drawn, flag = _axis_minutes([first, last])
        assert flag is continuous and len(drawn) == (span if continuous else 2)


def test_a_span_over_a_day_draws_only_the_minutes_with_rows(tmp_path: Path) -> None:
    far = T0 + (MAX_CONTINUOUS_MINUTES + 5) * 60_000
    data = clean()
    data["raw_events"].append(raw(500, "page_view", far, event_date="2026-01-02"))
    data["minute_aggregates"].append(aggregate(far, "page_view", 1, 0))
    page = render(collect(run_dir(tmp_path, data), GENERATED))
    assert "only minutes with rows are drawn" in page
    events_chart = page.split("<h2>Revenue per minute")[0]
    assert len(re.findall(r'<rect class="s1"', events_chart)) == 4
    # Over days, a label needs its date.
    assert ">2026-01-02 00:05</text>" in events_chart and "<title>2026-01-02 00:05: events 1</title>" in events_chart


def test_a_window_no_calendar_holds_is_shown_by_its_number(tmp_path: Path) -> None:
    """The quality rules report such a window; the page must still be made, and say what is there."""
    data = clean()
    data["minute_aggregates"].append(aggregate(10**16, "page_view", 1, 0))
    r = collect(run_dir(tmp_path, data), GENERATED)
    assert not r.quality.ok
    page = render(r)
    assert "10,000,000,000,000,000 ms" in page and "only minutes with rows are drawn" in page


def test_small_revenue_ticks_keep_their_cents(tmp_path: Path) -> None:
    data = clean()
    data["minute_aggregates"][0] = aggregate(revenue=10)  # ten cents
    page = render(collect(run_dir(tmp_path, data), GENERATED))
    revenue_chart = page.split("<h2>Revenue per minute")[1]
    assert ">$0.05</text>" in revenue_chart and ">$0.10</text>" in revenue_chart


def test_a_backlog_says_its_event_time_latency_isnt_the_pipelines(tmp_path: Path) -> None:
    data = clean()
    # Produced three days after the events happened (a replayed backlog).
    data["raw_events"] = [{**row, "kafka_timestamp": "2026-01-04 00:00:00+00"} for row in data["raw_events"]]
    page = render(collect(run_dir(tmp_path, data), GENERATED))
    assert "were produced 3 days after their event times" in page
    live = render(collect(run_dir(tmp_path / "live", clean()), GENERATED))
    assert "days after their event times" not in live


def test_a_bar_chart_stacks_or_groups(tmp_path: Path) -> None:
    labels = ["a", "b"]
    two = [Series("one", "s1", (1.0, 0.0)), Series("two", "s2", (2.0, 3.0))]
    stacked = bar_chart("t", labels, two, stacked=True)
    grouped = bar_chart("t", labels, two, stacked=False)
    # A zero draws no bar; both name both series in a legend and every bar's title.
    assert stacked.count("<rect") == grouped.count("<rect") == 3
    assert '<span><i class="s1"></i>one</span><span><i class="s2"></i>two</span>' in stacked
    assert "<title>a: one 1, two 2</title>" in stacked
    # Stacked, the axis reaches the tallest total (3); grouped, the tallest bar (3) too here, but a's bars sit side
    # by side: their x positions differ.
    xs = re.findall(r'<rect class="s[12]" x="([0-9.]+)"', grouped)
    assert xs[0] != xs[1]
    xs = re.findall(r'<rect class="s[12]" x="([0-9.]+)"', stacked)
    assert xs[0] == xs[1]
    # Stacked, a's second bar sits on its first: its bottom is the first's top.
    (y1, h1), (y2, h2) = [
        (float(y), float(h)) for y, h in re.findall(r'y="([0-9.]+)" width="[0-9.]+" height="([0-9.]+)"', stacked)[:2]
    ]
    assert y2 + h2 == pytest.approx(y1, abs=0.11) and h2 == pytest.approx(2 * h1, abs=0.11)
    assert '<p class="legend">' not in bar_chart("t", labels, two[:1], stacked=True)  # one series: no legend


BENCH = [
    {
        "bench": "sizes",
        "at": "2026-10-26T15:42:18-0400",
        "rows": [
            {
                "size": 300,
                "records": 11969,
                "wall_s": 101.0,
                "records_per_wall_s": 119,
                "raw_events_batches": 41,
                "dropped_late": 1280,
                "dropped_late_pct": 10.694,
                "predicted_dropped": 1280,
                "check_ok": True,
            }
        ],
    },
    {
        "bench": "triggers",
        "at": "2026-10-27T15:30:00-0400",
        "rows": [
            {
                "trigger_s": t,
                "sent_s": 300.0,
                "slept_s": -0.0 if t == 5 else 0.0,  # as bench.py can record it: shown as 0
                "records": 112000,
                "raw_events_batches": 300 // int(t),
                "produced_ms": {"count": 112000, "p50": 900.0 * t, "p95": 1600.0 * t, "p99": 3500.0 * t, "max": 1.0},
                "event_ms": {"count": 112000, "p50": 1000.0 * t, "p95": 1.0, "p99": 13000.0, "max": 1.0},
                "window_ms": {"count": 30, "p50": 184500.0, "p95": 1.0, "p99": 1.0, "max": 1.0} if t < 10 else None,
            }
            for t in (1.0, 5.0, 10.0)
        ],
    },
]


def test_benchmark_results_are_tabled(tmp_path: Path) -> None:
    bench = tmp_path / "results.json"
    bench.write_text(json.dumps(BENCH))
    r = collect(run_dir(tmp_path / "run", clean()), GENERATED, bench=bench)
    page = render(r)
    assert "Batch size over a backlog (2026-10-26T15:42:18-0400)" in page
    assert "<td class=num>1,280 (10.694%)</td><td class=num>1,280</td><td class=num>ok</td>" in page
    assert "<td>5 s</td><td class=num>112,000</td><td class=num>60</td><td class=num>0</td>" in page
    assert "<td class=num>4,500</td><td class=num>8,000</td><td class=num>17,500</td>" in page
    # A trigger whose run closed no window has an empty cell, not a crash.
    assert "<td class=num>13,000</td><td class=num></td></tr>" in page
    chart = page.split("Trigger interval on a live stream")[1]
    assert chart.count('<rect class="o') == 9 and "produced to queryable p99" in chart


def test_the_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = run_dir(tmp_path / "run", clean())
    assert main(["report", "--out", str(root)]) == 0
    assert capsys.readouterr().out == f"wrote {root / 'report.html'} (quality ok)\n"
    assert (root / "report.html").read_text().startswith("<!doctype html>")
    page = tmp_path / "pages" / "run.html"
    assert main(["report", "--out", str(root), "--html", str(page)]) == 0
    assert page.exists() and not list(page.parent.glob(".*"))  # written by a rename: no temporary left
    capsys.readouterr()
    assert main(["report", "--out", str(tmp_path / "missing")]) == 2
    assert "no run directory there" in capsys.readouterr().err
    bad = tmp_path / "bad.json"
    bad.write_text("[{")
    assert main(["report", "--out", str(root), "--bench", str(bad)]) == 1
    assert "bad.json: not JSON" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("results", "error"),
    [
        ({"bench": "sizes"}, "not a list of benchmark runs"),
        (["sizes"], "run 0 has no list of rows"),
        ([{"bench": "sizes", "rows": [{"size": 300}]}], "run 0 (sizes): a row without size, raw_events_batches"),
        ([{"bench": "sizes", "rows": [{**BENCH[0]["rows"][0], "wall_s": "101"}]}], "a row without"),
        ([{"bench": "sizes", "rows": [{**BENCH[0]["rows"][0], "check_ok": 1}]}], "a row without check_ok"),
        ([{"bench": "triggers", "rows": [{**BENCH[1]["rows"][0], "slept_s": float("nan")}]}], "a row without"),
        ([{"bench": "triggers", "rows": [{**BENCH[1]["rows"][0], "event_ms": {"p50": 1}}]}], "event_ms isn't"),
    ],
)
def test_a_results_file_bench_py_didnt_write_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], results: object, error: str
) -> None:
    root = run_dir(tmp_path / "run", clean())
    bench = tmp_path / "results.json"
    bench.write_text(json.dumps(results))
    assert main(["report", "--out", str(root), "--bench", str(bench)]) == 1
    assert error in capsys.readouterr().err
    assert not (root / "report.html").exists()


def test_runs_of_another_kind_are_skipped(tmp_path: Path) -> None:
    bench = tmp_path / "results.json"
    bench.write_text(json.dumps([{"bench": "someday", "rows": [{"x": 1}]}, BENCH[0]]))
    r = collect(run_dir(tmp_path / "run", clean()), GENERATED, bench=bench)
    assert [run["bench"] for run in r.bench] == ["sizes"]
