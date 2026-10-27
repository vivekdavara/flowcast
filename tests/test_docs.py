"""The documents against the code: every command they show parses, every subcommand and script they name exists."""

import re
import shlex
import tomllib
from pathlib import Path

import pytest

from flowcast import __version__
from flowcast.cli import build_parser

ROOT = Path(__file__).resolve().parent.parent
DOCS = ("README.md", "DESIGN.md", "WALKTHROUGH.md", "CHANGELOG.md")


def _text(name: str) -> str:
    path = ROOT / name
    return path.read_text() if path.exists() else ""


def _subcommands() -> set[str]:
    parser = build_parser()
    action = next(a for a in parser._actions if a.dest == "command")  # the subparsers
    return set(action.choices or {})


def _command_lines() -> list[tuple[str, str]]:
    """Every `.venv/bin/flowcast ...` line inside the documents' bash blocks."""
    lines = []
    for name in DOCS:
        for block in re.findall(r"```bash\n(.*?)```", _text(name), re.S):
            lines += [(name, line.strip()) for line in block.splitlines() if line.startswith(".venv/bin/flowcast ")]
    return lines


def test_the_documents_show_commands() -> None:
    assert len(_command_lines()) >= 15  # the extraction finds them (16 on day 5)


@pytest.mark.parametrize(("doc", "line"), _command_lines())
def test_every_command_shown_parses(doc: str, line: str) -> None:
    argv = shlex.split(line)[1:]
    try:
        build_parser().parse_args(argv)
    except SystemExit as exc:  # argparse's way of refusing
        pytest.fail(f"{doc}: `{line}` doesn't parse (exit {exc.code})")


def test_every_subcommand_named_exists() -> None:
    known = _subcommands()
    for name in DOCS:
        for command in re.findall(r"\bflowcast ([a-z][a-z-]*)\b", _text(name)):
            assert command in known, (name, command)


def test_every_script_named_exists() -> None:
    for name in DOCS:
        for script in re.findall(r"\bscripts/[A-Za-z0-9_.-]+\.(?:py|sh)\b", _text(name)):
            assert (ROOT / script).is_file(), (name, script)


def test_the_version_is_the_same_everywhere() -> None:
    """pyproject's version, the package's and the changelog's latest release are one version."""
    project = tomllib.loads(_text("pyproject.toml"))["project"]["version"]
    latest = re.search(r"^## v(\S+)", _text("CHANGELOG.md"), re.M)
    assert latest is not None
    assert project == __version__ == latest.group(1)
