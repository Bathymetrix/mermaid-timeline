#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

"""Dev-facing validation for the external BIN-to-CYCLE adapter."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import shutil
import sys
import tempfile


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mermaid_records.bin2cycle import Bin2CycleConfig, derive_workspace_cycles
from mermaid_records.bin2log import decode_workspace_logs, prepare_decode_workspace
from mermaid_records.operational_raw import iter_cycle_events
from mermaid_records.discovery import iter_bin_files


@dataclass(slots=True)
class SampleReport:
    """Validation summary for one BIN sample."""

    bin_file: Path
    identifier: str
    decode_success: bool
    decode_error: str | None
    decoded_line_count: int
    decoded_parse_success: bool
    decoded_parse_error: str | None
    decoded_parsed_entry_count: int | None
    processed_cycle_found: bool
    processed_cycle_file: Path | None
    processed_parse_success: bool | None
    processed_parse_error: str | None
    processed_line_count: int | None
    processed_parsed_entry_count: int | None


def main() -> int:
    """Run validation across BIN fixtures."""

    parser = argparse.ArgumentParser(
        description="Validate external BIN-to-CYCLE decoding against processed CYCLE.h references.",
    )
    parser.add_argument(
        "--fixtures-root",
        type=Path,
        default=REPO_ROOT / "data" / "fixtures",
        help="Directory holding the BIN fixtures to validate.",
    )
    parser.add_argument(
        "--processed-root",
        type=Path,
        required=True,
        help="Processed root under which matching .CYCLE.h files will be searched.",
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
        "--preflight-mode",
        choices=("strict", "cached"),
        default="strict",
        help="BIN decode preflight policy: strict requires successful live refresh; cached warns and continues on cached decoder state.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of BIN samples to validate.",
    )
    args = parser.parse_args()

    config = Bin2CycleConfig(
        python_executable=args.decoder_python,
        decoder_script=args.decoder_script,
        preflight_mode=args.preflight_mode,
    )
    bins = sorted(iter_bin_files(args.fixtures_root))[: args.limit]
    cycle_index = _index_processed_cycles(args.processed_root)
    reports = validate_samples(bins, config=config, cycle_index=cycle_index)
    _print_reports(reports)
    return 0


def validate_samples(
    paths: list[Path],
    *,
    config: Bin2CycleConfig,
    cycle_index: dict[str, list[Path]],
) -> list[SampleReport]:
    """Validate a batch of BIN samples against decoded and processed cycle text."""

    decoded_cycle_index: dict[str, list[str]] = {}
    batch_decode_error: str | None = None

    try:
        with tempfile.TemporaryDirectory(prefix="mermaid-cycle-validate-batch-") as tmpdir:
            workdir = Path(tmpdir)
            for path in paths:
                shutil.copy2(path, workdir / path.name)
            prepare_decode_workspace(workdir, config=config, refresh_database=True)
            decode_workspace_logs(workdir, config=config)
            for cycle_path in derive_workspace_cycles(workdir, config=config):
                identifier = _extract_identifier(cycle_path)
                decoded_cycle_index[identifier] = cycle_path.read_text(
                    encoding="utf-8"
                ).splitlines()
    except Exception as exc:  # pragma: no cover - real workflow path
        batch_decode_error = repr(exc)

    reports: list[SampleReport] = []
    for path in paths:
        identifier = _extract_identifier(path)
        processed_matches = cycle_index.get(identifier, [])
        processed_cycle = sorted(processed_matches)[0] if processed_matches else None

        decoded_lines = decoded_cycle_index.get(identifier, [])
        decode_success = batch_decode_error is None and identifier in decoded_cycle_index
        decode_error = None if decode_success else batch_decode_error
        decoded_parse_success = False
        decoded_parse_error: str | None = None
        decoded_parsed_entry_count: int | None = None

        if decode_success:
            try:
                decoded_parsed_entry_count = _parse_cycle_line_count(decoded_lines)
            except Exception as exc:  # pragma: no cover - real workflow path
                decoded_parse_error = repr(exc)
            else:
                decoded_parse_success = True

        processed_parse_success: bool | None = None
        processed_parse_error: str | None = None
        processed_line_count: int | None = None
        processed_parsed_entry_count: int | None = None

        if processed_cycle is not None:
            processed_lines = processed_cycle.read_text(encoding="utf-8").splitlines()
            processed_line_count = len(processed_lines)
            try:
                processed_parsed_entry_count = sum(1 for _ in iter_cycle_events(processed_cycle))
            except Exception as exc:  # pragma: no cover - real workflow path
                processed_parse_success = False
                processed_parse_error = repr(exc)
            else:
                processed_parse_success = True

        reports.append(
            SampleReport(
                bin_file=path,
                identifier=identifier,
                decode_success=decode_success,
                decode_error=decode_error,
                decoded_line_count=len(decoded_lines),
                decoded_parse_success=decoded_parse_success,
                decoded_parse_error=decoded_parse_error,
                decoded_parsed_entry_count=decoded_parsed_entry_count,
                processed_cycle_found=processed_cycle is not None,
                processed_cycle_file=processed_cycle,
                processed_parse_success=processed_parse_success,
                processed_parse_error=processed_parse_error,
                processed_line_count=processed_line_count,
                processed_parsed_entry_count=processed_parsed_entry_count,
            )
        )

    return reports


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


def _extract_identifier(path: Path) -> str:
    """Extract the shared hex-like identifier from a fixture filename."""

    basename = path.name.split(".", maxsplit=1)[0]
    return basename.split("_", maxsplit=1)[1]


def _index_processed_cycles(root: Path) -> dict[str, list[Path]]:
    """Index processed .CYCLE.h files by shared identifier."""

    index: dict[str, list[Path]] = {}
    for path in root.rglob("*.CYCLE.h"):
        identifier = _extract_identifier(path)
        index.setdefault(identifier, []).append(path)
    return index


def _parse_cycle_line_count(lines: list[str]) -> int:
    """Feed decoded cycle text through the existing cycle parser via a temp file."""

    with tempfile.TemporaryDirectory(prefix="mermaid-cycle-validate-") as tmpdir:
        path = Path(tmpdir) / "decoded.CYCLE"
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return sum(1 for _ in iter_cycle_events(path))


def _print_reports(reports: list[SampleReport]) -> None:
    """Print per-sample diagnostics and a short aggregate summary."""

    print("BIN->CYCLE validation")
    print()
    for report in reports:
        print(f"BIN: {report.bin_file}")
        print(f"  identifier: {report.identifier}")
        print(f"  decode_success: {report.decode_success}")
        if report.decode_error:
            print(f"  decode_error: {report.decode_error}")
        print(f"  decoded_line_count: {report.decoded_line_count}")
        print(f"  decoded_parse_success: {report.decoded_parse_success}")
        if report.decoded_parse_error:
            print(f"  decoded_parse_error: {report.decoded_parse_error}")
        if report.decoded_parsed_entry_count is not None:
            print(f"  decoded_parsed_entry_count: {report.decoded_parsed_entry_count}")
        print(f"  processed_cycle_found: {report.processed_cycle_found}")
        if report.processed_cycle_file is not None:
            print(f"  processed_cycle_file: {report.processed_cycle_file}")
        if report.processed_parse_success is not None:
            print(f"  processed_parse_success: {report.processed_parse_success}")
        if report.processed_parse_error:
            print(f"  processed_parse_error: {report.processed_parse_error}")
        if report.processed_line_count is not None:
            print(f"  processed_line_count: {report.processed_line_count}")
        if report.processed_parsed_entry_count is not None:
            print(f"  processed_parsed_entry_count: {report.processed_parsed_entry_count}")
        print()

    total = len(reports)
    decode_ok = sum(1 for report in reports if report.decode_success)
    decoded_parse_ok = sum(1 for report in reports if report.decoded_parse_success)
    processed_found = sum(1 for report in reports if report.processed_cycle_found)
    processed_parse_ok = sum(1 for report in reports if report.processed_parse_success)

    print("Summary")
    print(f"  samples: {total}")
    print(f"  decode_successes: {decode_ok}")
    print(f"  decoded_parse_successes: {decoded_parse_ok}")
    print(f"  processed_cycle_found: {processed_found}")
    print(f"  processed_parse_successes: {processed_parse_ok}")


if __name__ == "__main__":
    raise SystemExit(main())
