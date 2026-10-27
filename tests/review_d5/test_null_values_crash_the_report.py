"""Review of day 5 (27d22ac): a NULL the quality rules exist to flag crashes `flowcast report` with a traceback.

DESIGN.md "The report (day 5)": "Odd input is shown, not fatal". The rules report a window with no window_start and a
raw event with no event_time or kafka_timestamp ("required fields set"), so `flowcast quality` runs and says FAIL. But
`report.collect` dies on each of them with a TypeError, twice over:

- `latency.latencies` (called by collect, which catches only SinkLogError) computes `committed_ms - (start + 60000)`,
  `committed_ms - event_ms` and `committed_ms - kafka_ms` with the NULL as None;
- with latencies out of the way, collect's own `Minute(int(start), ...)` is `int(None)`.

TypeError is not among the errors `cli.main` turns into "flowcast report: ..." and exit 1, so the command prints a
traceback. Correct: the page is written (the quality section showing the failing rule), exit 0, as for every other
value the rules flag; at worst exit 1 with a one-line reason.
"""

from pathlib import Path

import pytest

from flowcast import report
from flowcast.cli import main
from flowcast.quality import check_quality
from tests.test_quality import T0, aggregate, clean, raw
from tests.test_report import GENERATED, run_dir


def _null_window_start() -> dict[str, list[dict[str, object]]]:
    data = clean()
    row = aggregate(T0 + 180_000, "page_view", 1, 0)
    row["window_start"] = None
    data["minute_aggregates"].append(row)
    return data


def _null_raw(column: str) -> dict[str, list[dict[str, object]]]:
    data = clean()
    data["raw_events"].append(raw(500, **{column: None}))
    return data


CASES = {
    "window_start": _null_window_start,
    "event_time": lambda: _null_raw("event_time"),
    "kafka_timestamp": lambda: _null_raw("kafka_timestamp"),
}


@pytest.mark.parametrize("column", sorted(CASES))
def test_a_null_the_rules_flag_still_gives_a_page(tmp_path: Path, column: str) -> None:
    root = run_dir(tmp_path / "run", CASES[column]())
    assert "required fields set" in [r.name for r in check_quality(root).results if not r.ok]
    page = tmp_path / "page.html"
    # At 27d22ac this raises TypeError out of main (a traceback), instead of returning an exit code.
    assert main(["report", "--out", str(root), "--html", str(page)]) == 0
    assert "required fields set" in page.read_text()


def test_collect_itself_survives_a_null_window_start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Even with the latency step out of the way, collect's own `int(start)` fails on the NULL window."""
    root = run_dir(tmp_path / "run", _null_window_start())
    monkeypatch.setattr(report, "latencies", lambda _root: "not measured here")
    r = report.collect(root, GENERATED)
    assert not r.quality.ok
    report.render(r)
