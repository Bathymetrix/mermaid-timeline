"""Command line interface for mermaid_timeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from .cycle_raw import iter_cycle_events
from .mer_raw import iter_mer_records


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""

    parser = argparse.ArgumentParser(prog="mermaid-timeline")
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

    for record in iter_mer_records(args.path):
        time_text = record.timestamp.isoformat() if record.timestamp else "-"
        print(f"{time_text}\t{record.record_type}\t{len(record.payload)}")
    return 0


def _handle_inspect_cycle(args: argparse.Namespace) -> int:
    """Handle the inspect-cycle subcommand."""

    for event in iter_cycle_events(args.path):
        time_text = event.timestamp.isoformat() if event.timestamp else "-"
        print(f"{time_text}\t{event.event_type}\t{event.message}")
    return 0
