"""The `flowcast` command line."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from flowcast import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flowcast", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version", help="print the version")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "version":
        print(f"flowcast {__version__}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
