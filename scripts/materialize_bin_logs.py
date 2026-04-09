#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

"""Decode BIN fixtures into LOG fixtures and verify them against tracked LOGs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mermaid_timeline.bin2log import Bin2LogConfig, iter_decoded_log_lines


@dataclass(slots=True)
class LogMaterializationReport:
    """Decode and verification result for one BIN fixture."""

    bin_file: Path
    expected_log_file: Path
    decode_success: bool
    decode_error: str | None
    decoded_line_count: int
    wrote_log_file: bool
    expected_log_exists: bool
    matches_expected_log: bool | None


def main() -> int:
    """Decode BIN fixtures into LOG files and verify them against tracked LOGs."""

    parser = argparse.ArgumentParser(
        description="Materialize BIN fixtures into LOG files and verify them against tracked LOG fixtures.",
    )
    parser.add_argument(
        "--family-root",
        type=Path,
        default=REPO_ROOT / "data" / "fixtures" / "467.174-T-0100",
        help="Fixture family root containing bin/ and log/ directories.",
    )
    parser.add_argument(
        "--decoder-python",
        type=Path,
        required=True,
        help="Python executable for the external manufacturer decoder environment.",
    )
    parser.add_argument(
        "--decoder-script",
        type=Path,
        required=True,
        help="Path to external preprocess.py decoder script.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of BIN files to process.",
    )
    parser.add_argument(
        "--write-missing",
        action="store_true",
        help="Write decoded LOG output only when the expected fixture LOG is missing.",
    )
    args = parser.parse_args()

    config = Bin2LogConfig(
        python_executable=args.decoder_python,
        decoder_script=args.decoder_script,
    )
    reports = materialize_family(
        args.family_root,
        config=config,
        limit=args.limit,
        write_missing=args.write_missing,
    )
    _print_reports(reports)
    return 0


def materialize_family(
    family_root: Path,
    *,
    config: Bin2LogConfig,
    limit: int | None,
    write_missing: bool,
) -> list[LogMaterializationReport]:
    """Decode BIN fixtures for one family and compare against tracked LOGs."""

    bin_root = family_root / "bin"
    log_root = family_root / "log"
    bin_paths = sorted(bin_root.glob("*.BIN"))
    if limit is not None:
        bin_paths = bin_paths[:limit]

    reports: list[LogMaterializationReport] = []
    for bin_path in bin_paths:
        expected_log = log_root / f"{bin_path.stem}.LOG"
        decoded_lines: list[str] = []
        decode_success = False
        decode_error: str | None = None
        wrote_log_file = False
        matches_expected_log: bool | None = None
        expected_exists = expected_log.exists()

        try:
            decoded_lines = list(iter_decoded_log_lines(bin_path, config=config))
            decode_success = True
        except Exception as exc:  # pragma: no cover - real workflow path
            decode_error = repr(exc)
        else:
            decoded_text = "\n".join(decoded_lines) + ("\n" if decoded_lines else "")
            if expected_exists:
                matches_expected_log = (
                    expected_log.read_text(encoding="utf-8") == decoded_text
                )
            elif write_missing:
                expected_log.parent.mkdir(parents=True, exist_ok=True)
                expected_log.write_text(decoded_text, encoding="utf-8")
                wrote_log_file = True
                matches_expected_log = True

        reports.append(
            LogMaterializationReport(
                bin_file=bin_path,
                expected_log_file=expected_log,
                decode_success=decode_success,
                decode_error=decode_error,
                decoded_line_count=len(decoded_lines),
                wrote_log_file=wrote_log_file,
                expected_log_exists=expected_exists,
                matches_expected_log=matches_expected_log,
            )
        )

    return reports


def _print_reports(reports: list[LogMaterializationReport]) -> None:
    """Print per-file results and a short aggregate summary."""

    print("BIN->LOG fixture materialization")
    print()
    for report in reports:
        print(f"BIN: {report.bin_file}")
        print(f"  expected_log_file: {report.expected_log_file}")
        print(f"  decode_success: {report.decode_success}")
        if report.decode_error:
            print(f"  decode_error: {report.decode_error}")
        print(f"  decoded_line_count: {report.decoded_line_count}")
        print(f"  expected_log_exists: {report.expected_log_exists}")
        print(f"  wrote_log_file: {report.wrote_log_file}")
        if report.matches_expected_log is not None:
            print(f"  matches_expected_log: {report.matches_expected_log}")
        print()

    total = len(reports)
    decode_ok = sum(1 for report in reports if report.decode_success)
    expected_present = sum(1 for report in reports if report.expected_log_exists)
    matched = sum(1 for report in reports if report.matches_expected_log is True)
    mismatched = sum(1 for report in reports if report.matches_expected_log is False)
    written = sum(1 for report in reports if report.wrote_log_file)

    print("Summary")
    print(f"  samples: {total}")
    print(f"  decode_successes: {decode_ok}")
    print(f"  expected_log_present: {expected_present}")
    print(f"  matched_expected_log: {matched}")
    print(f"  mismatched_expected_log: {mismatched}")
    print(f"  wrote_missing_logs: {written}")


if __name__ == "__main__":
    raise SystemExit(main())
