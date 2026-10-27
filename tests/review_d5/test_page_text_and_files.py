"""Review of day 5 (27d22ac): smaller defects in what the page prints and where it writes.

1. Money isn't exact past 2**53 cents. `_dollars` formats `cents / 100`, a float, so the tile and the revenue tables
   print $90,071,992,547,409.94 for 9,007,199,254,740,993 cents (exact: $90,071,992,547,409.93), though DESIGN.md
   says money is integer cents summed exactly (DuckDB's sum is a HUGEINT, and `flowcast query` prints the exact
   cents). `_n` has the same float conversion for counts (`f"{10**17 + 1:,.0f}"` is "100,000,000,000,000,000").
   Correct: integer formatting (divmod by 100), not float.

2. A run whose windows all lie at a time no calendar holds, within a day of each other (e.g. one window at 10**16 ms,
   which the rules flag), is drawn on the continuous axis, where a label is `_clock(ms)[11:]`: the time-of-day slice
   of "10,000,000,000,000,000 ms", i.e. "000,000,000 ms". Correct: such a minute is labelled by its whole number (as
   the table and the non-continuous axis do).

3. `write` leaves its temporary file behind when the rename fails: `--html` naming an existing directory exits 1
   ("Is a directory") and leaves `.NAME.tmp` next to it. Correct: no temporary file is left after a failed write.
"""

import re
from pathlib import Path

from flowcast.cli import main
from flowcast.report import _dollars, _n, collect, render
from tests.test_quality import aggregate, clean
from tests.test_report import GENERATED, run_dir


def test_money_and_counts_are_printed_exactly() -> None:
    assert _dollars(2**53 + 1) == "$90,071,992,547,409.93"
    assert _n(10**17 + 1) == "100,000,000,000,000,001"


def test_a_window_no_calendar_holds_is_labelled_by_its_number_on_a_continuous_axis(tmp_path: Path) -> None:
    data = clean()
    data["minute_aggregates"] = [aggregate(10**16, "page_view", 1, 0)]
    page = render(collect(run_dir(tmp_path, data), GENERATED))
    labels = re.findall(r'text-anchor="middle">([^<]*)</text>', page)
    assert labels and all(label == "10,000,000,000,000,000 ms" for label in labels), labels


def test_a_failed_write_leaves_no_temporary_file(tmp_path: Path) -> None:
    root = run_dir(tmp_path / "run", clean())
    target = tmp_path / "pages"
    target.mkdir()
    assert main(["report", "--out", str(root), "--html", str(target)]) == 1
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []  # ['.pages.tmp'] at 27d22ac
