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


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""

    parser = argparse.ArgumentParser(
        prog="mermaid-records",
        description="Bathymetrix™ CLI for the MERMAID normalization pipeline.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    normalize = subparsers.add_parser(
        "normalize",
        help="Run the normalization pipeline from raw inputs to JSONL outputs.",
    )
    input_group = normalize.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "-i",
        "--input-root",
        type=Path,
        help="Root directory containing BIN, LOG, and/or MER inputs.",
    )
    input_group.add_argument(
        "--input-file",
        nargs="+",
        type=str,
        action="append",
        default=None,
        help="Explicit raw source file(s) to normalize in stateless mode. Accepts comma-separated and/or space-separated lists.",
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
        input_files=_parse_input_files(args.input_file),
    )
    print(json.dumps(summary.to_dict(), sort_keys=True))
    return 0


def _parse_input_files(values: list[list[str]] | None) -> list[Path] | None:
    """Flatten repeated --input-file arguments into a list of paths."""

    if not values:
        return None

    paths: list[Path] = []
    for group in values:
        for item in group:
            for token in item.split(","):
                token = token.strip()
                if token:
                    paths.append(Path(token))
    return paths


if __name__ == "__main__":
    raise SystemExit(main())
