"""Review of day 5 (27d22ac): on a continuous axis a window that doesn't start on a minute vanishes from the page.

`_axis_minutes` draws `first + i * 60000` for i in range(span) and looks each up in a dict of the minutes; a window
whose start isn't on that grid (the rule "window starts on a minute" flags it) is never looked up, so its events are in
neither the chart nor the table under it, while the tile "minutes in the windows" counts it. With clean() plus a
window at T0 + 1 ms holding 5 page views, the events table sums to 102 while
`flowcast query "SELECT sum(events) FROM minute_aggregates"` gives 107.

Correct: every minute with rows is in the per-minute table (and drawn), whatever its start; the table's events sum to
the view's sum, as DESIGN.md says the page shows "the same numbers as the terminal". (Drawing such a run on the
"only minutes with rows" axis, as for an out-of-calendar window, would do.)
"""

import re
from pathlib import Path

from flowcast.report import collect, render
from flowcast.views import connect
from tests.test_quality import T0, aggregate, clean
from tests.test_report import GENERATED, run_dir


def test_every_window_is_in_the_per_minute_table(tmp_path: Path) -> None:
    data = clean()
    data["minute_aggregates"].append(aggregate(T0 + 1, "page_view", 5, 0))
    root = run_dir(tmp_path, data)
    r = collect(root, GENERATED)
    assert not r.quality.ok  # "window starts on a minute" flags it
    page = render(r)
    events_section = page.split("<h2>Revenue per minute")[0]
    cells = re.findall(r"<tr><td>[^<]*</td><td class=num>([0-9,-]+)</td></tr>", events_section)
    con = connect(root)
    try:
        row = con.execute("SELECT sum(events), count(DISTINCT window_start) FROM minute_aggregates").fetchone()
    finally:
        con.close()
    assert row is not None
    assert "<b>4</b><span>minutes in the windows</span>" in page  # the tile counts it
    assert sum(int(c.replace(",", "")) for c in cells) == row[0]  # 102 != 107 at 27d22ac
    assert len(cells) >= row[1]
