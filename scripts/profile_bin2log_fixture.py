#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

"""Profile the existing BIN->LOG batch decode workflow on a fixture subset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mermaid_records.bin2log import Bin2LogConfig, Bin2LogError, update_decoder_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Profile the current BIN->LOG batch decode path on a fixture subset.",
    )
    parser.add_argument(
        "count",
        type=int,
        help="Number of BIN fixtures to include from the family root.",
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
        "--preflight-mode",
        choices=("strict", "cached"),
        default="strict",
        help="BIN decode preflight policy: strict requires successful live refresh; cached warns and continues on cached decoder state.",
    )
    args = parser.parse_args()

    config = Bin2LogConfig(
        python_executable=args.decoder_python,
        decoder_script=args.decoder_script,
        preflight_mode=args.preflight_mode,
    )
    summary = profile_fixture_family(
        family_root=args.family_root,
        count=args.count,
        config=config,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


def profile_fixture_family(
    *,
    family_root: Path,
    count: int,
    config: Bin2LogConfig,
) -> dict[str, object]:
    """Profile the real batch decode path over a limited BIN fixture subset."""

    if count <= 0:
        raise ValueError("count must be positive")

    bin_root = family_root / "bin"
    bin_paths = sorted(bin_root.glob("*.BIN"))[:count]
    if not bin_paths:
        raise FileNotFoundError(f"No .BIN files found under {bin_root}")

    phase_seconds = {
        "workspace_create": 0.0,
        "copy_bin_inputs": 0.0,
        "database_update": 0.0,
        "concatenate_files": 0.0,
        "decrypt_all": 0.0,
        "scan_log_outputs": 0.0,
        "read_log_outputs": 0.0,
        "cleanup": 0.0,
        "total": 0.0,
    }

    total_started = time.perf_counter()
    workdir: Path | None = None
    tmpdir: tempfile.TemporaryDirectory[str] | None = None
    log_paths: list[Path] = []
    log_total_bytes = 0
    error: dict[str, str] | None = None

    try:
        started = time.perf_counter()
        tmpdir = tempfile.TemporaryDirectory(prefix="mermaid-bin2log-profile-")
        workdir = Path(tmpdir.name)
        phase_seconds["workspace_create"] = time.perf_counter() - started

        started = time.perf_counter()
        for bin_path in bin_paths:
            shutil.copy2(bin_path, workdir / bin_path.name)
        phase_seconds["copy_bin_inputs"] = time.perf_counter() - started

        started = time.perf_counter()
        update_decoder_database(config)
        phase_seconds["database_update"] = time.perf_counter() - started

        started = time.perf_counter()
        _run_preprocess_phase(workdir, config=config, function_names=["concatenate_files", "concatenate_rbr_files"])
        phase_seconds["concatenate_files"] = time.perf_counter() - started

        started = time.perf_counter()
        _run_preprocess_phase(workdir, config=config, function_names=["decrypt_all"])
        phase_seconds["decrypt_all"] = time.perf_counter() - started

        started = time.perf_counter()
        log_paths = sorted(workdir.glob("*.LOG"))
        phase_seconds["scan_log_outputs"] = time.perf_counter() - started

        started = time.perf_counter()
        for log_path in log_paths:
            log_total_bytes += len(log_path.read_bytes())
        phase_seconds["read_log_outputs"] = time.perf_counter() - started
    except Exception as exc:
        error = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
    finally:
        started = time.perf_counter()
        if tmpdir is not None:
            tmpdir.cleanup()
        phase_seconds["cleanup"] = time.perf_counter() - started
        phase_seconds["total"] = time.perf_counter() - total_started

    return {
        "fixture_family_root": family_root.as_posix(),
        "bin_root": bin_root.as_posix(),
        "requested_count": count,
        "bin_count": len(bin_paths),
        "total_bin_bytes": sum(path.stat().st_size for path in bin_paths),
        "log_count": len(log_paths),
        "total_log_bytes": log_total_bytes,
        "bin_files": [path.name for path in bin_paths],
        "phase_seconds": phase_seconds,
        "error": error,
    }


def _run_preprocess_phase(
    workdir: Path,
    *,
    config: Bin2LogConfig,
    function_names: list[str],
) -> None:
    """Invoke one or more real preprocess functions in the existing decoder environment."""

    harness = """
from pathlib import Path
import os
import runpy
import sys

decoder_script = Path(sys.argv[1]).resolve()
workdir = Path(sys.argv[2]).resolve()
function_names = sys.argv[3:]

sys.path.insert(0, str(decoder_script.parent))
namespace = runpy.run_path(str(decoder_script))

workdir_str = str(workdir) + os.sep
for function_name in function_names:
    function = namespace.get(function_name)
    if callable(function):
        function(workdir_str)
"""
    result = subprocess.run(
        [
            str(config.python_executable),
            "-c",
            harness,
            str(config.decoder_script),
            str(workdir),
            *function_names,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise Bin2LogError(_format_subprocess_failure(result.returncode, result.stdout, result.stderr))


def _format_subprocess_failure(returncode: int, stdout: str, stderr: str) -> str:
    """Format a subprocess failure message consistently."""

    stderr_text = stderr.strip()
    stdout_text = stdout.strip()
    detail = stderr_text or stdout_text or f"exit code {returncode}"
    return f"External decoder failed: {detail}"


if __name__ == "__main__":
    raise SystemExit(main())
