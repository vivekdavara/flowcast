"""report.py: tests for the mutants the day-5 review's mutation pass found surviving (75 run, 42 survived at 27d22ac).
The equivalent or cosmetic ones (the table-wrap, the tooltip's escaping of names the code makes, the bar gap, the
label spacing, the write's atomicity) have none."""

import dataclasses
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from flowcast.cli import main
from flowcast.layout import Paths
from flowcast.report import Series, bar_chart, collect, render
from tests.test_latency import report as progress
from tests.test_quality import T0, aggregate, clean, dead
from tests.test_report import BENCH, GENERATED, publish_backfill, run_dir
from tests.test_views import log_file


def test_a_minute_only_in_the_stream_has_a_backfill_of_0(tmp_path: Path) -> None:
    root = run_dir(tmp_path, clean())
    publish_backfill(root, [aggregate(), aggregate(T0 + 60_000, "page_view", 1, 0)])  # nothing at T0 + 120 s
    page = render(collect(root, GENERATED))
    assert "<td>2026-01-01 00:02</td><td class=num>100</td><td class=num>0</td><td class=num>-100</td>" in page
    assert "In 1 minute the backfill has fewer events than the stream" in page


def test_a_cleaned_up_sink_log_is_not_measured_and_says_why(tmp_path: Path) -> None:
    root = run_dir(tmp_path / "a<b", clean())
    sink = root / "raw_events"
    log = sink / "_spark_metadata"
    entries = [line for line in (log / "0").read_text().splitlines()[1:]]
    (log / "0").unlink()
    (log / "4.compact").write_text("\n".join(["v1", *entries]))  # batches 0-3 cleaned up after a compact
    log_file(sink, "5", [])
    page = render(collect(root, GENERATED))
    assert "<p>Not measured: " in page and "a&lt;b" in page and "a<b" not in page.split("Not measured: ")[1][:300]


def test_throughput_rows_for_every_query_with_progress(tmp_path: Path) -> None:
    root = run_dir(tmp_path, clean())
    paths = Paths(root)
    paths.progress("raw_events").parent.mkdir(parents=True)
    paths.progress("raw_events").write_text(
        progress(0, 3_000, "2026-10-26T19:00:00.000Z", 1_500)
        + "\n"
        + progress(1, 1_000, "2026-10-26T19:00:02.000Z", 500)
    )
    paths.progress("duckdb_aggregates").write_text(progress(0, 10, "2026-10-26T19:00:00.000Z", 100) + "\n")
    page = render(collect(root, GENERATED))
    # rows, batches, span (s), rows/s over the span, rows per busy second
    assert "<td>raw_events</td><td class=num>4,000</td><td class=num>2</td><td class=num>2.5</td>" in page
    assert "<td class=num>2.5</td><td class=num>1,600</td><td class=num>2,000</td>" in page
    assert "<td>duckdb_aggregates</td><td class=num>10</td>" in page


def test_a_failed_check_is_shown_failed_and_its_lines_escaped(tmp_path: Path) -> None:
    root = run_dir(tmp_path, clean())
    check = SimpleNamespace(lines=lambda: ["minute_aggregates: <1 missing>", "MISMATCH"], ok=False)
    r = collect(root, GENERATED, check=check)  # type: ignore[arg-type]  # collect reads only lines() and ok
    assert r.check == (("minute_aggregates: <1 missing>", "MISMATCH"), False)
    page = render(r)
    assert 'exact check <span class="status bad">✕ MISMATCH</span>' in page
    assert '<h2>Exact check <span class="status bad">✕ MISMATCH</span></h2>' in page
    assert "<pre>minute_aggregates: &lt;1 missing&gt;\nMISMATCH</pre>" in page
    passed = render(dataclasses.replace(r, check=(("ok",), True)))
    assert '<h2>Exact check <span class="status good">✓ ok</span></h2>' in passed


def test_each_rule_row_shows_its_own_verdict(tmp_path: Path) -> None:
    data = clean()
    data["raw_events"][2] = {**data["raw_events"][2], "device": "watch"}
    page = render(collect(run_dir(tmp_path, data), GENERATED))
    assert '<tr><td><span class="status bad">✕ FAIL</span></td><td>raw_events</td><td>known device</td>' in page
    assert '<tr><td><span class="status good">✓ ok</span></td><td>raw_events</td><td>known event type</td>' in page


@pytest.mark.parametrize(
    ("dead_letters", "verdict", "rate"),
    [(0, "✓ ok", "0.0000%"), (1, "✓ ok", "0.9615%"), (2, "✕ FAIL", "1.9048%")],
)
def test_the_dead_letter_ceiling_row(tmp_path: Path, dead_letters: int, verdict: str, rate: str) -> None:
    data = clean()
    data["dead_letters"] = [dead(200 + i) for i in range(dead_letters)]
    page = render(collect(run_dir(tmp_path, data), GENERATED))
    row = re.search(
        r"<tr><td><span[^>]*>([^<]*)</span></td><td>dead_letters</td><td>at most 1.00% of records</td>"
        r"<td class=num>([^<]*)</td>",
        page,
    )
    assert row is not None and row.groups() == (verdict, rate)


def test_an_empty_latency_row_counts_0_rows(tmp_path: Path) -> None:
    page = render(collect(tmp_path, GENERATED))
    assert "<td>produced to queryable (raw_events)</td><td class=num>0</td><td class=num></td>" in page


def test_the_backlog_note_starts_after_a_day(tmp_path: Path) -> None:
    data = clean()
    data["raw_events"] = [{**row, "kafka_timestamp": "2026-01-02 12:00:00+00"} for row in data["raw_events"]]
    page = render(collect(run_dir(tmp_path, data), GENERATED))
    assert "were produced 1.5 days after their event times" in page  # 36 hours less the events' 2 minutes


def test_stacked_axes_reach_the_total_and_grouped_bars_stand_on_the_baseline() -> None:
    two = [Series("one", "s1", (1.0,)), Series("two", "s2", (2.0,))]
    assert ">3</text>" in bar_chart("t", ["a"], two, stacked=True)
    grouped = bar_chart("t", ["a"], two, stacked=False)
    assert ">3</text>" not in grouped
    for y, h in re.findall(r'y="([0-9.]+)" width="[0-9.]+" height="([0-9.]+)"', grouped):
        assert float(y) + float(h) == pytest.approx(210, abs=0.11)  # top 10 + plot height 200


def test_whole_dollar_ticks_have_no_cents(tmp_path: Path) -> None:
    page = render(collect(run_dir(tmp_path, clean()), GENERATED))
    revenue = page.split("<h2>Revenue per minute")[1]
    assert ">$5</text>" in revenue and ">$10</text>" in revenue


def _bench(tmp_path: Path, runs: object) -> list[str]:
    root = run_dir(tmp_path / "run", clean())
    path = tmp_path / "results.json"
    path.write_text(json.dumps(runs) if not isinstance(runs, bytes) else "")
    if isinstance(runs, bytes):
        path.write_bytes(runs)
    return ["report", "--out", str(root), "--bench", str(path)]


@pytest.mark.parametrize(
    "key",
    [
        "size",
        "raw_events_batches",
        "wall_s",
        "records_per_wall_s",
        "dropped_late",
        "dropped_late_pct",
        "predicted_dropped",
    ],
)
def test_each_sizes_key_is_required(tmp_path: Path, key: str, capsys: pytest.CaptureFixture[str]) -> None:
    row = {k: v for k, v in BENCH[0]["rows"][0].items() if k != key}
    assert main(_bench(tmp_path, [{"bench": "sizes", "rows": [row]}])) == 1
    assert "a row without" in capsys.readouterr().err


@pytest.mark.parametrize(
    "change",
    [{"size": True}, {"window_ms": {}}, {"produced_ms": {"count": 1, "p50": 1, "p95": 1, "p99": 1}}],
)
def test_bools_and_partial_summaries_are_refused(tmp_path: Path, change: dict[str, object]) -> None:
    kind = 0 if "size" in change else 1
    row = {**BENCH[kind]["rows"][0], **change}
    assert main(_bench(tmp_path, [{"bench": BENCH[kind]["bench"], "rows": [row]}])) == 1


def test_a_results_file_that_isnt_utf8_is_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(_bench(tmp_path, b"\xff\xfe[]")) == 1
    assert "not JSON" in capsys.readouterr().err


def test_bench_tables_show_a_failed_check_and_escape_the_stamp(tmp_path: Path) -> None:
    sizes = {**BENCH[0], "at": "<b>today</b>", "rows": [{**BENCH[0]["rows"][0], "check_ok": False}]}
    args = _bench(tmp_path, [sizes, BENCH[1]])
    page = tmp_path / "page.html"
    assert main([*args, "--html", str(page)]) == 0
    text = page.read_text()
    assert "<td class=num>MISMATCH</td>" in text and "(&lt;b&gt;today&lt;/b&gt;)" in text


def test_no_trigger_chart_when_any_run_has_no_produced_latency(tmp_path: Path) -> None:
    rows = [BENCH[1]["rows"][0], {**BENCH[1]["rows"][1], "produced_ms": None}]
    args = _bench(tmp_path, [{**BENCH[1], "rows": rows}])
    page = tmp_path / "page.html"
    assert main([*args, "--html", str(page)]) == 0
    assert '<rect class="o' not in page.read_text()
