#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

"""Profile the end-to-end normalization pipeline on a fixture root."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mermaid_records.bin2log import Bin2LogConfig, decode_workspace_logs, prepare_decode_workspace
from mermaid_records.discovery import iter_bin_files, iter_log_files, iter_mer_files
from mermaid_records.mer_raw import parse_mer_file
from mermaid_records.normalize_log import (
    _classify_acquisition,
    _classify_ascent_request,
    _classify_gps,
    _classify_measurement,
    _classify_transmission,
    write_log_jsonl_prototypes,
)
from mermaid_records.normalize_mer import (
    _build_data_record,
    _build_environment_record,
    _build_parameter_record,
    write_mer_jsonl_prototypes,
)
from mermaid_records.operational_raw import iter_operational_log_entries


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Profile the end-to-end normalization pipeline on a fixture root.",
    )
    parser.add_argument(
        "fixture_root",
        type=Path,
        help="Root containing BIN / LOG / MER fixture inputs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional output directory for JSONL outputs. Defaults to a temporary directory.",
    )
    parser.add_argument(
        "--decoder-python",
        type=Path,
        default=None,
        help="Python executable for the external manufacturer decoder environment.",
    )
    parser.add_argument(
        "--decoder-script",
        type=Path,
        default=None,
        help="Path to external preprocess.py decoder script.",
    )
    parser.add_argument(
        "--preflight-mode",
        choices=("strict", "cached"),
        default="strict",
        help="BIN decode preflight policy: strict requires successful live refresh; cached warns and continues on cached decoder state.",
    )
    args = parser.parse_args()

    config = None
    if args.decoder_python is not None or args.decoder_script is not None:
        if args.decoder_python is None or args.decoder_script is None:
            raise SystemExit("--decoder-python and --decoder-script must be provided together")
        config = Bin2LogConfig(
            python_executable=args.decoder_python,
            decoder_script=args.decoder_script,
            preflight_mode=args.preflight_mode,
        )

    summary = profile_pipeline(
        fixture_root=args.fixture_root,
        output_dir=args.output_dir,
        config=config,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


def profile_pipeline(
    *,
    fixture_root: Path,
    output_dir: Path | None,
    config: Bin2LogConfig | None,
) -> dict[str, object]:
    """Profile the end-to-end normalization pipeline on one fixture root."""

    phase_seconds = {
        "discover_inputs": 0.0,
        "decode_bin_to_log": 0.0,
        "parse_log": 0.0,
        "normalize_log": 0.0,
        "write_log_jsonl": 0.0,
        "parse_mer": 0.0,
        "normalize_mer": 0.0,
        "write_mer_jsonl": 0.0,
        "total": 0.0,
    }

    total_started = time.perf_counter()
    temp_output_dir: tempfile.TemporaryDirectory[str] | None = None
    decoded_workspace: tempfile.TemporaryDirectory[str] | None = None
    output_root: Path | None = None
    error_type: str | None = None
    error_message: str | None = None

    bin_paths: list[Path] = []
    discovered_log_paths: list[Path] = []
    mer_paths: list[Path] = []
    decoded_log_paths: list[Path] = []
    all_log_paths: list[Path] = []
    log_record_count = 0
    mer_environment_record_count = 0
    mer_parameter_record_count = 0
    mer_data_record_count = 0
    total_bin_bytes = 0
    total_log_bytes = 0
    total_mer_bytes = 0

    try:
        started = time.perf_counter()
        bin_paths = sorted(iter_bin_files(fixture_root))
        discovered_log_paths = sorted(iter_log_files(fixture_root))
        mer_paths = sorted(iter_mer_files(fixture_root))
        total_bin_bytes = sum(path.stat().st_size for path in bin_paths)
        total_mer_bytes = sum(path.stat().st_size for path in mer_paths)
        phase_seconds["discover_inputs"] = time.perf_counter() - started

        if bin_paths:
            if config is None:
                raise ValueError("decoder config is required when BIN inputs are present")
            started = time.perf_counter()
            decoded_workspace = tempfile.TemporaryDirectory(prefix="mermaid-normalize-decode-")
            workdir = Path(decoded_workspace.name)
            for path in bin_paths:
                shutil.copy2(path, workdir / path.name)
            prepare_decode_workspace(workdir, config=config, refresh_database=True)
            decoded_log_paths = decode_workspace_logs(workdir, config=config)
            phase_seconds["decode_bin_to_log"] = time.perf_counter() - started

        all_log_paths = sorted({path.resolve(): path for path in [*discovered_log_paths, *decoded_log_paths]}.values())
        total_log_bytes = sum(path.stat().st_size for path in all_log_paths)

        started = time.perf_counter()
        for path in all_log_paths:
            for entry in iter_operational_log_entries(path):
                if entry.source_kind == "log":
                    log_record_count += 1
        phase_seconds["parse_log"] = time.perf_counter() - started

        started = time.perf_counter()
        for path in all_log_paths:
            for entry in iter_operational_log_entries(path):
                if entry.source_kind != "log":
                    continue
                _classify_acquisition(entry)
                _classify_ascent_request(entry)
                _classify_gps(entry)
                _classify_transmission(entry)
                _classify_measurement(entry)
        phase_seconds["normalize_log"] = time.perf_counter() - started

        if output_dir is None:
            temp_output_dir = tempfile.TemporaryDirectory(prefix="mermaid-normalize-output-")
            output_root = Path(temp_output_dir.name)
        else:
            output_root = output_dir
            output_root.mkdir(parents=True, exist_ok=True)

        started = time.perf_counter()
        log_summary = write_log_jsonl_prototypes(all_log_paths, output_root / "log_jsonl")
        phase_seconds["write_log_jsonl"] = time.perf_counter() - started

        started = time.perf_counter()
        parsed_mer_files: list[tuple[Path, object, object]] = []
        for path in mer_paths:
            metadata, blocks = parse_mer_file(path)
            parsed_mer_files.append((path, metadata, blocks))
        phase_seconds["parse_mer"] = time.perf_counter() - started

        started = time.perf_counter()
        for path, metadata, blocks in parsed_mer_files:
            float_id = path.stem.split("_", maxsplit=1)[0]
            for line in metadata.raw_environment_lines:
                _build_environment_record(float_id=float_id, path=path, line=line)
                mer_environment_record_count += 1
            for line in metadata.raw_parameter_lines:
                _build_parameter_record(float_id=float_id, path=path, line=line)
                mer_parameter_record_count += 1
            for block_index, block in enumerate(blocks):
                _build_data_record(
                    float_id=float_id,
                    path=path,
                    block_index=block_index,
                    raw_info_line=block.raw_info_line,
                    raw_format_line=block.raw_format_line,
                    data_payload=block.data_payload,
                )
                mer_data_record_count += 1
        phase_seconds["normalize_mer"] = time.perf_counter() - started

        started = time.perf_counter()
        mer_summary = write_mer_jsonl_prototypes(mer_paths, output_root / "mer_jsonl")
        phase_seconds["write_mer_jsonl"] = time.perf_counter() - started
    except Exception as exc:
        error_type = type(exc).__name__
        error_message = str(exc)
    finally:
        phase_seconds["total"] = time.perf_counter() - total_started
        if decoded_workspace is not None:
            decoded_workspace.cleanup()
        if temp_output_dir is not None:
            temp_output_dir.cleanup()

    result = {
        "root": fixture_root.as_posix(),
        "bin_count": len(bin_paths),
        "log_count": len(discovered_log_paths),
        "mer_count": len(mer_paths),
        "decoded_log_count": len(decoded_log_paths),
        "total_bin_bytes": total_bin_bytes,
        "total_log_bytes": total_log_bytes,
        "total_mer_bytes": total_mer_bytes,
        "discover_inputs_s": phase_seconds["discover_inputs"],
        "decode_bin_to_log_s": phase_seconds["decode_bin_to_log"],
        "parse_log_s": phase_seconds["parse_log"],
        "normalize_log_s": phase_seconds["normalize_log"],
        "write_log_jsonl_s": phase_seconds["write_log_jsonl"],
        "parse_mer_s": phase_seconds["parse_mer"],
        "normalize_mer_s": phase_seconds["normalize_mer"],
        "write_mer_jsonl_s": phase_seconds["write_mer_jsonl"],
        "log_record_count": log_record_count,
        "mer_environment_record_count": mer_environment_record_count,
        "mer_parameter_record_count": mer_parameter_record_count,
        "mer_data_record_count": mer_data_record_count,
        "total_s": phase_seconds["total"],
    }
    if error_type is not None:
        result["error_type"] = error_type
        result["error_message"] = error_message
    return result


if __name__ == "__main__":
    raise SystemExit(main())
