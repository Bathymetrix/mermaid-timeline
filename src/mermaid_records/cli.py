# Bathymetrix™
# https://bathymetrix.com
# © 2026 Bathymetrix, LLC
# Author: Joel D. Simon <jdsimon@bathymetrix.com>
# SPDX-License-Identifier: MIT

"""Command line interface for mermaid_records."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .bin2log import Bin2LogConfig
from .normalize_pipeline import run_normalization_pipeline
from .operational_raw import iter_operational_log_entries
from .mer_raw import iter_mer_data_blocks


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""

    parser = argparse.ArgumentParser(
        prog="mermaid-records",
        description="Bathymetrix™ CLI for conservative MERMAID raw parsing.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_mer = subparsers.add_parser("inspect-mer", help="Inspect raw MER records.")
    inspect_mer.add_argument("path", type=Path, help="Path to a MER file.")
    inspect_mer.set_defaults(handler=_handle_inspect_mer)

    inspect_cycle = subparsers.add_parser(
        "inspect-cycle",
        help="Inspect parsed operational LOG/CYCLE/CYCLE.h events.",
    )
    inspect_cycle.add_argument(
        "path",
        type=Path,
        help="Path to a LOG, CYCLE, or CYCLE.h file.",
    )
    inspect_cycle.set_defaults(handler=_handle_inspect_cycle)

    normalize = subparsers.add_parser(
        "normalize",
        help="Run the normalization pipeline from raw inputs to JSONL outputs.",
    )
    normalize.add_argument(
        "-i",
        "--input-root",
        type=Path,
        required=True,
        help="Root directory containing BIN, LOG, and/or MER inputs.",
    )
    normalize.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where normalized JSONL outputs will be written.",
    )
    normalize.add_argument(
        "--decoder-python",
        type=Path,
        default=None,
        help="Python executable for the external manufacturer decoder environment.",
    )
    normalize.add_argument(
        "--decoder-script",
        type=Path,
        default=None,
        help="Path to external preprocess.py decoder script.",
    )
    normalize.add_argument(
        "--preflight-mode",
        choices=("strict", "cached"),
        default="strict",
        help="BIN decode preflight policy: strict requires successful live refresh; cached warns and continues on cached decoder state.",
    )
    normalize.set_defaults(handler=_handle_normalize)

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

    for entry in iter_operational_log_entries(args.path):
        code_text = entry.code or "-"
        print(
            f"{entry.time.isoformat()}\t"
            f"{entry.source_kind}\t"
            f"{entry.subsystem}:{code_text}\t"
            f"{entry.message}"
        )
    return 0


def _handle_normalize(args: argparse.Namespace) -> int:
    """Handle the normalize subcommand."""

    config = None
    if args.decoder_python is not None or args.decoder_script is not None:
        if args.decoder_python is None or args.decoder_script is None:
            raise SystemExit("--decoder-python and --decoder-script must be provided together")
        config = Bin2LogConfig(
            python_executable=args.decoder_python,
            decoder_script=args.decoder_script,
            preflight_mode=args.preflight_mode,
        )

    summary = run_normalization_pipeline(
        args.input_root,
        output_dir=args.output_dir,
        config=config,
    )
    print(json.dumps(summary.to_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
