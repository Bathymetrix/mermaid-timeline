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
    normalize.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute the normalization plan and file-level diffs without writing files.",
    )
    normalize.add_argument(
        "--json",
        action="store_true",
        help="Print dry-run output as structured JSON instead of a human-readable plan.",
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
        dry_run=args.dry_run,
    )
    payload = summary.to_dict()
    if args.dry_run and not args.json:
        print(_format_dry_run(payload))
    else:
        print(json.dumps(payload, sort_keys=True))
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


def _format_dry_run(payload: dict[str, object]) -> str:
    lines: list[str] = []
    for float_payload in payload["floats"]:
        counts = float_payload["counts"]
        lines.append(f"FLOAT {Path(float_payload['output_dir']).name}")
        lines.append(
            "  files: "
            f"total={counts['total']} new={counts['new']} changed={counts['changed']} "
            f"removed={counts['removed']} unchanged={counts['unchanged']}"
        )
        for family_name in ("log", "mer"):
            family = float_payload["families"][family_name]
            lines.append(f"  {family_name}: {family['action']}")
            for change_kind in ("new", "changed", "removed"):
                rows = [row for row in family["file_diffs"] if row["change_kind"] == change_kind]
                if not rows:
                    continue
                lines.append(f"    {change_kind}:")
                for row in rows:
                    lines.append(f"      - {_format_diff_row(row)}")
            if family["decoder_invalidated"]:
                lines.append("    decoder-invalidated:")
                for row in family["decoder_invalidated"]:
                    lines.append(f"      - {_format_diff_row(row)}")
    return "\n".join(lines)


def _format_diff_row(row: dict[str, object]) -> str:
    name = Path(row["source_file"]).name
    previous_size = int(row["previous_size_bytes"])
    current_size = int(row["current_size_bytes"])
    if row["change_kind"] == "changed":
        return f"{name} (hash changed, {previous_size} B -> {current_size} B)"
    return f"{name} ({previous_size} B -> {current_size} B)"


if __name__ == "__main__":
    raise SystemExit(main())
