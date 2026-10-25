import json
from pathlib import Path

import duckdb
import pytest

from flowcast.views import COLUMNS, SinkLogError, committed_files, connect, data_files, latest_batch, orphan_files


def log_file(sink: Path, name: str, entries: list[tuple[Path | str, str]], version: str = "v1") -> None:
    log = sink / "_spark_metadata"
    log.mkdir(parents=True, exist_ok=True)
    lines = [version]
    for path, action in entries:
        uri = path if isinstance(path, str) else path.as_uri()
        lines.append(json.dumps({"path": uri, "size": 1, "isDir": False, "action": action}))
    (log / name).write_text("\n".join(lines))


def test_no_log_is_no_files(tmp_path: Path) -> None:
    assert committed_files(tmp_path / "missing") == []
    assert committed_files(tmp_path) == []


def test_the_batch_files_list_the_committed_files(tmp_path: Path) -> None:
    a, b, c = (tmp_path / f"part-{n}.parquet" for n in "abc")
    log_file(tmp_path, "0", [(a, "add"), (b, "add")])
    log_file(tmp_path, "1", [(c, "add")])
    (tmp_path / "_spark_metadata" / ".1.crc").write_text("")
    (tmp_path / "_spark_metadata" / ".2.6f1e.tmp").write_text("v1\n")  # a batch still being logged
    assert committed_files(tmp_path) == [a, b, c]


def test_the_latest_compact_file_replaces_the_files_before_it(tmp_path: Path) -> None:
    files = [tmp_path / f"part-{n}.parquet" for n in range(4)]
    log_file(tmp_path, "8", [(tmp_path / "gone.parquet", "add")])  # older than the compact file: not read
    log_file(tmp_path, "9.compact", [(files[0], "add"), (files[1], "add")])
    log_file(tmp_path, "10", [(files[2], "add"), (files[0], "delete")])
    log_file(tmp_path, "11", [(files[3], "add")])
    assert committed_files(tmp_path) == [files[1], files[2], files[3]]


def test_a_percent_encoded_uri_is_decoded(tmp_path: Path) -> None:
    path = tmp_path / "event_date=2026-01-01" / "part 0.parquet"
    log_file(tmp_path, "0", [(path, "add")])
    assert committed_files(tmp_path) == [path]


@pytest.mark.parametrize(
    ("names", "reason"),
    [
        (["0", "2"], "consecutive"),
        (["1"], "consecutive"),
        (["9.compact", "11"], "consecutive"),
        (["0", "notes.txt"], "not a sink log file"),
    ],
)
def test_a_log_with_a_gap_or_a_stranger_is_refused(tmp_path: Path, names: list[str], reason: str) -> None:
    for name in names:
        log_file(tmp_path, name, [])
    with pytest.raises(SinkLogError, match=reason):
        committed_files(tmp_path)


@pytest.mark.parametrize(
    ("entries", "version", "reason"),
    [
        ([], "v2", "version-1"),
        ([("hdfs://namenode/part-0.parquet", "add")], "v1", "isn't a local file"),
        ([("file:///part-0.parquet", "rename")], "v1", "unknown action"),
    ],
)
def test_an_entry_it_cant_read_is_refused(
    tmp_path: Path, entries: list[tuple[str, str]], version: str, reason: str
) -> None:
    log_file(tmp_path, "0", entries, version)  # type: ignore[arg-type]
    with pytest.raises(SinkLogError, match=reason):
        committed_files(tmp_path)


def test_a_sink_with_nothing_committed_is_an_empty_view_with_the_right_columns(tmp_path: Path) -> None:
    con = connect(tmp_path)
    try:
        for name, columns in COLUMNS.items():
            assert con.execute(f"SELECT count(*) FROM {name}").fetchone() == (0,)
            described = con.execute(f"DESCRIBE {name}").fetchall()
            assert [(row[0], row[1]) for row in described] == [
                (column, kind.replace("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")) for column, kind in columns.items()
            ]
    finally:
        con.close()


def test_a_view_reads_the_logged_files_and_not_a_stray_one(tmp_path: Path) -> None:
    sink = tmp_path / "minute_aggregates"
    sink.mkdir()
    logged, stray = sink / "part-0.parquet", sink / "part-1.parquet"
    row = (
        "SELECT {}::BIGINT AS window_start, 'page_view' AS event_type, {}::BIGINT AS events, 0::BIGINT AS revenue_cents"
    )
    with duckdb.connect() as con:
        con.execute(f"COPY ({row.format(0, 3)}) TO '{logged}'")
        con.execute(f"COPY ({row.format(60_000, 99)}) TO '{stray}'")  # left by a batch that failed
    log_file(sink, "0", [(logged, "add")])
    con = connect(tmp_path)
    try:
        assert con.execute("SELECT * FROM minute_aggregates").fetchall() == [(0, "page_view", 3, 0)]
    finally:
        con.close()


def test_a_numberless_file_name_is_refused(tmp_path: Path) -> None:
    log_file(tmp_path, "0", [])
    log_file(tmp_path, "abc", [])
    with pytest.raises(SinkLogError, match="not a sink log file"):
        committed_files(tmp_path)


def test_the_latest_batch_counts_compact_files(tmp_path: Path) -> None:
    assert latest_batch(tmp_path) == -1
    log_file(tmp_path, "9.compact", [])
    assert latest_batch(tmp_path) == 9  # Spark writes 9.compact instead of 9 at a compaction batch
    log_file(tmp_path, "10", [])
    assert latest_batch(tmp_path) == 10


def test_only_the_newest_compact_file_and_the_batches_after_it_are_read(tmp_path: Path) -> None:
    old, new, stale, last = (tmp_path / f"{n}.parquet" for n in ("old", "new", "stale", "last"))
    log_file(tmp_path, "9.compact", [(old, "add")])
    log_file(tmp_path, "19.compact", [(new, "add")])
    log_file(tmp_path, "19", [(stale, "add")])  # a batch file beside its own compact file: the compact one rules
    log_file(tmp_path, "20", [(last, "add")])
    assert committed_files(tmp_path) == sorted([new, last])


def test_a_quote_in_a_path_is_escaped_in_the_view(tmp_path: Path) -> None:
    root = tmp_path / "Vivek's runs"
    sink = root / "minute_aggregates"
    file = sink / "part-0.parquet"
    sink.mkdir(parents=True)
    with duckdb.connect() as con:
        con.execute(
            "COPY (SELECT 0::BIGINT AS window_start, 'checkout' AS event_type, 1::BIGINT AS events, "
            "0::BIGINT AS revenue_cents) TO ?",
            [str(file)],
        )
    log_file(sink, "0", [(file, "add")])
    con = connect(root)
    try:
        assert con.execute("SELECT event_type FROM minute_aggregates").fetchall() == [("checkout",)]
    finally:
        con.close()


def test_orphan_files_are_the_data_files_no_batch_lists(tmp_path: Path) -> None:
    sink = tmp_path / "sink"
    logged = sink / "event_date=2026-01-01" / "part-a.parquet"
    orphan = sink / "event_date=2026-01-01" / "part-b.parquet"
    hidden = [
        sink / ".part-c.parquet.crc",
        sink / "_temporary" / "part-d.parquet",
        sink / "_temporary" / "0" / "part-e.parquet",
    ]
    for path in [logged, orphan, *hidden]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
    # The log names the file through another spelling of the same directory (on macOS, /var is /private/var).
    alias = tmp_path / "alias"
    alias.symlink_to(sink)
    log_file(sink, "0", [(alias / "event_date=2026-01-01" / "part-a.parquet", "add")])
    assert data_files(sink) == [logged, orphan]
    assert orphan_files(sink) == [orphan]
    # And the other way round: the sink reached through the other spelling, its log naming the real path.
    log_file(sink, "0", [(logged, "add")])
    assert orphan_files(alias) == [alias / "event_date=2026-01-01" / "part-b.parquet"]
    assert orphan_files(tmp_path / "missing") == []


def test_raw_files_from_before_and_after_schema_version_2_read_as_one_view(tmp_path: Path) -> None:
    # A job upgraded in place: its first batches wrote files with no campaign column, later ones with it.
    day = tmp_path / "raw_events" / "event_date=2026-01-01"
    day.mkdir(parents=True)
    old, new = day / "part-0.parquet", day / "part-1.parquet"
    v1_columns = ", ".join(
        f"{'0' if kind in ('INTEGER', 'BIGINT') else 'NULL'}::{kind} AS {column}"
        for column, kind in COLUMNS["raw_events"].items()
        if column not in ("campaign", "event_date")
    )
    with duckdb.connect() as con:
        con.execute(f"COPY (SELECT {v1_columns}) TO '{old}'")
        # Columns in another order, as a reader can't assume they line up.
        con.execute(f"COPY (SELECT 'newsletter' AS campaign, {v1_columns}) TO '{new}'")
    log_file(tmp_path / "raw_events", "0", [(old, "add")])
    log_file(tmp_path / "raw_events", "1", [(new, "add")])
    con = connect(tmp_path)
    try:
        rows = con.execute("SELECT campaign, event_date FROM raw_events ORDER BY campaign NULLS FIRST").fetchall()
        assert [(campaign, str(date)) for campaign, date in rows] == [
            (None, "2026-01-01"),
            ("newsletter", "2026-01-01"),
        ]
    finally:
        con.close()
