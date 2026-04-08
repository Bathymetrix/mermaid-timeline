#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

"""Dev-facing full-corpus BIN->CYCLE adapter audit."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sys
import tempfile


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mermaid_timeline.bin2cycle import Bin2CycleConfig, iter_decoded_cycle_lines
from mermaid_timeline.cycle_raw import iter_cycle_events
from mermaid_timeline.discovery import iter_bin_files


@dataclass(slots=True)
class BinAuditRecord:
    """Per-file BIN->CYCLE decode and parse audit result."""

    path: str
    decode_success: bool
    parse_success: bool
    decoded_line_count: int | None
    parsed_entry_count: int | None
    error_stage: str | None
    error_type: str | None
    error_message: str | None


def main() -> int:
    """Run a full-corpus decode/parse audit over raw BIN files."""

    parser = argparse.ArgumentParser(
        description="Audit BIN->CYCLE decode and cycle-parse results over a BIN corpus.",
    )
    parser.add_argument(
        "--bin-root",
        type=Path,
        required=True,
        help="Root directory under which raw .BIN files will be discovered recursively.",
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
        default=_default_decoder_script(),
        help="Path to external preprocess.py decoder script.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="Optional path to write a machine-readable JSON report.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of BIN files to audit.",
    )
    args = parser.parse_args()

    config = Bin2CycleConfig(
        python_executable=args.decoder_python,
        decoder_script=args.decoder_script,
    )

    paths = iter_bin_files(args.bin_root)
    if args.limit is not None:
        paths = _limited(paths, args.limit)

    records = [audit_bin(path, config=config) for path in paths]
    summary = summarize(records)

    print_summary(summary)
    if args.report_json is not None:
        payload = {
            "bin_root": str(args.bin_root),
            "summary": summary,
            "records": [asdict(record) for record in records],
        }
        args.report_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print()
        print(f"report_json: {args.report_json}")

    return 0


def audit_bin(path: Path, *, config: Bin2CycleConfig) -> BinAuditRecord:
    """Audit one BIN file through decode and cycle parsing."""

    decoded_lines: list[str] = []
    try:
        decoded_lines = list(iter_decoded_cycle_lines(path, config=config))
    except Exception as exc:  # pragma: no cover - real corpus workflow
        return BinAuditRecord(
            path=str(path),
            decode_success=False,
            parse_success=False,
            decoded_line_count=None,
            parsed_entry_count=None,
            error_stage="decode",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

    try:
        parsed_entry_count = _parse_cycle_line_count(decoded_lines)
    except Exception as exc:  # pragma: no cover - real corpus workflow
        return BinAuditRecord(
            path=str(path),
            decode_success=True,
            parse_success=False,
            decoded_line_count=len(decoded_lines),
            parsed_entry_count=None,
            error_stage="parse",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

    return BinAuditRecord(
        path=str(path),
        decode_success=True,
        parse_success=True,
        decoded_line_count=len(decoded_lines),
        parsed_entry_count=parsed_entry_count,
        error_stage=None,
        error_type=None,
        error_message=None,
    )


def summarize(records: list[BinAuditRecord]) -> dict[str, object]:
    """Build aggregate summary stats from per-file audit records."""

    total = len(records)
    decode_successes = sum(1 for record in records if record.decode_success)
    parse_successes = sum(1 for record in records if record.parse_success)
    failure_categories = Counter()

    for record in records:
        if record.error_stage is None:
            continue
        key = f"{record.error_stage}:{record.error_type}"
        failure_categories[key] += 1

    return {
        "total_files": total,
        "decode_successes": decode_successes,
        "decode_failures": total - decode_successes,
        "parse_successes": parse_successes,
        "parse_failures": total - parse_successes,
        "failure_counts_by_category": dict(sorted(failure_categories.items())),
    }


def print_summary(summary: dict[str, object]) -> None:
    """Print a compact aggregate summary."""

    print("BIN->CYCLE full-corpus audit")
    print(f"  total_files: {summary['total_files']}")
    print(f"  decode_successes: {summary['decode_successes']}")
    print(f"  decode_failures: {summary['decode_failures']}")
    print(f"  parse_successes: {summary['parse_successes']}")
    print(f"  parse_failures: {summary['parse_failures']}")
    print("  failure_counts_by_category:")
    failure_counts = summary["failure_counts_by_category"]
    if not failure_counts:
        print("    none")
        return
    for key, value in failure_counts.items():
        print(f"    {key}: {value}")


def _parse_cycle_line_count(lines: list[str]) -> int:
    """Feed decoded raw CYCLE text into the existing cycle parser unchanged."""

    with tempfile.TemporaryDirectory(prefix="mermaid-cycle-audit-") as tmpdir:
        path = Path(tmpdir) / "decoded.CYCLE"
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return sum(1 for _ in iter_cycle_events(path))


def _limited(paths: object, limit: int) -> list[Path]:
    """Materialize at most ``limit`` paths from an iterator."""

    result: list[Path] = []
    for path in paths:
        result.append(path)
        if len(result) >= limit:
            break
    return result


def _default_decoder_script() -> Path | None:
    """Read the decoder script path from the local corpus config when present."""

    local_file = REPO_ROOT / "data" / "fixtures" / "corpus_root.local.txt"
    if not local_file.exists():
        return None
    for line in local_file.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if text.endswith("preprocess.py"):
            return Path(text)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
