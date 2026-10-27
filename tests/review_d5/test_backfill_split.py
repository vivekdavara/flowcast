"""Review of day 5 (27d22ac): the backfill split when a minute's backfill is below the stream, and stacked bars.

1. `_events_section` clamps a minute's late events at 0 for the chart and the "N events in all" total, but the table's
   "late" column is the unclamped `backfill - stream`. A run whose backfill has fewer events than the stream in a
   minute (which the stream can't legitimately do; it is exactly what the page should make visible) shows "-10" in
   the table and "0 events in all" in the text above it, and the bar is the stream's 100 although the text says
   "Each bar is a minute's events in the backfill". Correct: the text and the table agree (the total is the sum of
   the late column, or the column clamps too and the minute is called out).

2. `bar_chart(stacked=True)` scales the axis to the sum of each label's values, negatives included, but draws only the
   positive ones. A minute whose stream count is negative (the rule "counts positive" flags it) with a backfill of 10
   stacks a late bar of 15 on an axis that ends at 10: the rect has y=-90 and height 300 in a 240-high viewBox, outside
   the plot. Correct: every bar lies inside the plot (the axis covers what is drawn).
"""

import re
from pathlib import Path

from flowcast.report import Series, bar_chart, collect, render
from tests.test_quality import T0, aggregate, clean
from tests.test_report import GENERATED, publish_backfill, run_dir

ROW = re.compile(
    r"<tr><td>([^<]*)</td><td class=num>([^<]*)</td><td class=num>([^<]*)</td><td class=num>([^<]*)</td></tr>"
)


def test_the_late_total_and_the_late_column_agree_when_the_backfill_is_below_the_stream(tmp_path: Path) -> None:
    root = run_dir(tmp_path, clean())
    publish_backfill(
        root, [aggregate(), aggregate(T0 + 60_000, "page_view", 1, 0), aggregate(T0 + 120_000, "page_view", 90, 0)]
    )
    page = render(collect(root, GENERATED))
    total = re.search(r"dropped as late: ([0-9,-]+) events in all", page)
    assert total is not None
    late_column = [int(late.replace(",", "")) for _, _, _, late in ROW.findall(page)]
    assert late_column == [0, 0, -10]
    # Resolved (day 5) by calling the minute out rather than netting it into the total: a total of late events that
    # subtracted another minute's shortfall would hide late records. At 27d22ac nothing on the page said so.
    assert int(total.group(1).replace(",", "")) == 0
    assert "In 1 minute the backfill has fewer events than the stream, which a correct run can't have" in page


def _rects(svg: str) -> list[tuple[float, float]]:
    return [
        (float(y), float(h))
        for y, h in re.findall(
            r'<rect class="[^"]*" x="[^"]*" y="([^"]*)" width="[^"]*" '
            r'height="([^"]*)"',
            svg,
        )
    ]


def test_stacked_bars_stay_inside_the_plot_with_a_negative_value() -> None:
    svg = bar_chart("t", ["a"], [Series("stream", "s1", (-5.0,)), Series("late", "s2", (15.0,))], stacked=True)
    rects = _rects(svg)
    assert rects
    for y, h in rects:
        assert y >= 0 and y + h <= 240, (y, h)  # y=-90, h=300 at 27d22ac


def test_the_page_of_such_a_run_keeps_its_bars_inside_the_plot(tmp_path: Path) -> None:
    data = clean()
    data["minute_aggregates"] = [aggregate(T0, "page_view", -5, 0)]
    root = run_dir(tmp_path, data)
    publish_backfill(root, [aggregate(T0, "page_view", 10, 0)])
    page = render(collect(root, GENERATED))
    for y, h in _rects(page.split("<h2>Revenue per minute")[0]):
        assert y >= 0 and y + h <= 240, (y, h)
