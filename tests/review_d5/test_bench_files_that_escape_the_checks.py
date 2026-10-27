"""Review of day 5 (27d22ac): results files that `load_bench` doesn't turn into a BenchError, so `flowcast report
--bench` ends in a traceback instead of "exit 1 with the reason" (DESIGN.md: "a bad file is exit 1 with the reason").

- An integer too big for a float (`"size": 1e400` written as digits): `_number` calls `math.isfinite`, which raises
  OverflowError.
- An integer of more than 4,300 digits: Python 3.11's `json.loads` raises ValueError ("Exceeds the limit (4300
  digits) for integer string conversion"), which is not a JSONDecodeError.
- Deep nesting (`[[[...]]]`, 100,000 levels): `json.loads` raises RecursionError.

None of OverflowError, plain ValueError or RecursionError is caught by `load_bench` or `cli.main`. Correct: each is a
BenchError, so the command prints "flowcast report: <file>: ..." and returns 1, and no page is written.
"""

from pathlib import Path

import pytest

from flowcast.cli import main
from tests.test_quality import clean
from tests.test_report import run_dir

ROW = (
    '"raw_events_batches":1,"wall_s":1,"records_per_wall_s":1,"dropped_late":0,"dropped_late_pct":0,'
    '"predicted_dropped":0,"check_ok":true'
)

FILES = {
    "int_too_big_for_a_float": '[{"bench":"sizes","rows":[{"size":1' + "0" * 400 + "," + ROW + "}]}]",
    "int_past_the_digit_limit": '[{"bench":"sizes","rows":[{"size":1' + "0" * 5000 + "," + ROW + "}]}]",
    "deeply_nested": "[" * 100_000 + "]" * 100_000,
}


@pytest.mark.parametrize("name", sorted(FILES))
def test_a_bad_results_file_is_exit_1_with_the_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], name: str
) -> None:
    root = run_dir(tmp_path / "run", clean())
    bench = tmp_path / "results.json"
    bench.write_text(FILES[name])
    # At 27d22ac main raises (OverflowError / ValueError / RecursionError) instead of returning.
    assert main(["report", "--out", str(root), "--bench", str(bench)]) == 1
    assert "results.json" in capsys.readouterr().err
    assert not (root / "report.html").exists()
