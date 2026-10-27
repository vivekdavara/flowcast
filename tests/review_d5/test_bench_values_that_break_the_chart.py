"""Review of day 5 (27d22ac): finite latencies that pass `load_bench` but break the trigger chart.

`load_bench` accepts any finite number, and `bar_chart` / `nice_ticks` then fail on the float extremes:

- p50 = 5e-324 (the smallest subnormal): `nice_ticks` divides by 4, gets 0.0, and `math.log10(0.0)` raises
  ValueError ("math domain error") while rendering: a traceback from `flowcast report --bench`.
- p50 = 1e-320 (any top in (0, 1e-9], e.g. 1e-10 ms): the trigger chart is an integer axis (bar_chart's default),
  so the step is raised to 1 and `math.ceil(top / 1 - 1e-9)` is 0: `nice_ticks(1e-10, integer=True)` is `[0.0]`, and
  `scale = plot_h / ticks[-1]` raises ZeroDivisionError (a zero-height plot). On a non-integer axis the same top gives
  scale = inf and nan/inf coordinates instead.
- p99 = 1.7e308: the step is the int 5 * 10**307, the top tick the int 2 * 10**308, and `tick * scale` converts it to
  a float: OverflowError while rendering.

DESIGN.md: "--bench files are checked against what bench.py writes ... before anything is drawn, so a bad file is
exit 1 with the reason". Correct: such a file is refused by load_bench (exit 1, the reason on stderr), or drawn with
finite coordinates; never a traceback, never nan/inf in the page.
"""

import json
import re
from pathlib import Path

import pytest

from flowcast.cli import main
from tests.test_quality import clean
from tests.test_report import run_dir


def _triggers(p50: float, p95: float, p99: float) -> list[dict[str, object]]:
    summary = {"count": 1, "p50": p50, "p95": p95, "p99": p99, "max": p99}
    row = {
        "trigger_s": 1.0,
        "sent_s": 1.0,
        "slept_s": 0.0,
        "records": 1,
        "raw_events_batches": 1,
        "produced_ms": summary,
        "event_ms": summary,
        "window_ms": None,
    }
    return [{"bench": "triggers", "at": "2026-10-27T15:30:00-0400", "rows": [row]}]


CASES = {
    "smallest_subnormal": (5e-324, 5e-324, 5e-324),
    "tiny": (1e-320, 1e-320, 1e-320),
    "near_float_max": (1.0, 1.0, 1.7e308),
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_extreme_finite_latencies_are_refused_or_drawn_finitely(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], name: str
) -> None:
    root = run_dir(tmp_path / "run", clean())
    bench = tmp_path / "results.json"
    bench.write_text(json.dumps(_triggers(*CASES[name])))
    page = tmp_path / "page.html"
    # At 27d22ac: ValueError (smallest_subnormal), ZeroDivisionError (tiny), OverflowError (near_float_max) out of
    # main, a traceback each.
    code = main(["report", "--out", str(root), "--bench", str(bench), "--html", str(page)])
    if code == 1:
        assert "results.json" in capsys.readouterr().err
        return
    assert code == 0
    numbers = re.findall(r'(?:x|y|x1|x2|y1|y2|width|height)="([^"]*)"', page.read_text())
    assert not [n for n in numbers if n in ("nan", "inf", "-inf")], "nan/inf in the SVG"


def test_an_integer_axis_below_1e_9_still_has_a_top_tick() -> None:
    from flowcast.report import nice_ticks

    ticks = nice_ticks(1e-10, integer=True)
    assert ticks[-1] > 0, ticks  # [0.0] at 27d22ac
