# Bathymetrix™
# https://bathymetrix.com
# © 2026 Bathymetrix, LLC
# Author: Joel D. Simon <jdsimon@bathymetrix.com>
# Licensed under the MIT License

"""Command line interface for mermaid_timeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from .cycle_raw import iter_cycle_events
from .mer_raw import iter_mer_data_blocks


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""

    parser = argparse.ArgumentParser(
        prog="mermaid-timeline",
        description="Bathymetrix™ CLI for conservative MERMAID raw parsing.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_mer = subparsers.add_parser("inspect-mer", help="Inspect raw MER records.")
    inspect_mer.add_argument("path", type=Path, help="Path to a MER file.")
    inspect_mer.set_defaults(handler=_handle_inspect_mer)

    inspect_cycle = subparsers.add_parser(
        "inspect-cycle",
        help="Inspect parsed .CYCLE.h events.",
    )
    inspect_cycle.add_argument("path", type=Path, help="Path to a .CYCLE.h file.")
    inspect_cycle.set_defaults(handler=_handle_inspect_cycle)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""

    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler")
    return handler(args)


def _handle_inspect_mer(args: argparse.Namespace) -> int:
    """Handle the inspect-mer subcommand."""

    for block in iter_mer_data_blocks(args.path):
        time_text = block.date.isoformat() if block.date else "-"
        payload_length = len(block.data_payload) if block.data_payload is not None else 0
        print(f"{time_text}\tEVENT\t{payload_length}")
    return 0


def _handle_inspect_cycle(args: argparse.Namespace) -> int:
    """Handle the inspect-cycle subcommand."""

    for entry in iter_cycle_events(args.path):
        code_text = entry.code or "-"
        print(f"{entry.time.isoformat()}\t{entry.subsystem}:{code_text}\t{entry.message}")
    return 0
