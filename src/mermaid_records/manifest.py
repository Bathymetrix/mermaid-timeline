# SPDX-License-Identifier: MIT

"""Manifest persistence for normalization pipeline runs."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from .bin2log import Bin2LogConfig
    from .normalize_pipeline import NormalizationPipelineSummary


RUN_STATUS = ("success", "partial", "failed")


def begin_run(
    *,
    input_root: Path,
    output_root: Path,
    config: Bin2LogConfig | None,
    raw_source_paths: list[Path],
) -> dict[str, object]:
    """Create the initial run context for manifest persistence."""

    started_at = _iso_now()
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    manifests_root = output_root / "manifests"
    run_dir = manifests_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    return {
        "run_id": run_id,
        "started_at": started_at,
        "input_root": input_root,
        "output_root": output_root,
        "manifests_root": manifests_root,
        "run_dir": run_dir,
        "source_state": build_source_state(raw_source_paths=raw_source_paths, config=config),
    }


def finalize_run(
    *,
    context: dict[str, object],
    normalization_version: str,
    preflight_mode: str | None,
    summary: NormalizationPipelineSummary | None,
    error: BaseException | None,
) -> None:
    """Write the manifest set for a completed or failed normalization run."""

    run_id = context["run_id"]
    started_at = context["started_at"]
    input_root = Path(context["input_root"])
    output_root = Path(context["output_root"])
    manifests_root = Path(context["manifests_root"])
    run_dir = Path(context["run_dir"])
    source_state = context["source_state"]

    completed_at = _iso_now()
    status = _run_status(output_root, error)

    run_json = {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "input_root": input_root.as_posix(),
        "output_root": output_root.as_posix(),
        "normalization_version": normalization_version,
        "preflight_mode": preflight_mode,
        "status": status,
    }
    outputs_json = _build_outputs_json(output_root=output_root, summary=summary)

    _write_json(run_dir / "run.json", run_json)
    _write_json(run_dir / "outputs.json", outputs_json)
    _write_json(run_dir / "source_state.json", source_state)

    preflight_root = output_root / "preflight_status.json"
    if preflight_root.exists():
        (run_dir / "preflight_status.json").write_text(
            preflight_root.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    latest_json = {
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "run_manifest": (run_dir / "run.json").relative_to(output_root).as_posix(),
        "outputs_manifest": (run_dir / "outputs.json").relative_to(output_root).as_posix(),
        "source_state_manifest": (run_dir / "source_state.json").relative_to(output_root).as_posix(),
        "preflight_status": (
            (run_dir / "preflight_status.json").relative_to(output_root).as_posix()
            if (run_dir / "preflight_status.json").exists()
            else None
        ),
    }
    manifests_root.mkdir(parents=True, exist_ok=True)
    _write_json(manifests_root / "latest.json", latest_json)


def build_source_state(
    *,
    raw_source_paths: list[Path],
    config: Bin2LogConfig | None,
) -> dict[str, object]:
    """Build persisted source state for raw inputs and decoder state."""

    raw_sources = [
        {
            "source_file": path.as_posix(),
            "source_kind": _source_kind(path),
            "size_bytes": path.stat().st_size,
            "content_hash": _hash_file(path),
        }
        for path in sorted(raw_source_paths)
    ]
    return {
        "raw_sources": raw_sources,
        "decoder_state": _decoder_state(config),
    }


def _decoder_state(config: Bin2LogConfig | None) -> dict[str, object] | None:
    if config is None:
        return None

    decoder_python_version = _python_version(config.python_executable)
    decoder_script_hash = _hash_file(config.decoder_script)
    decoder_git_commit = _git_commit(config.decoder_script.parent)
    database_root = _database_root()
    database_files = _database_files(database_root)
    database_bundle_hash = _bundle_hash(database_files)

    return {
        "decoder_python": str(config.python_executable),
        "decoder_python_version": decoder_python_version,
        "decoder_script": str(config.decoder_script),
        "decoder_script_hash": decoder_script_hash,
        "preflight_mode": config.preflight_mode,
        "database_bundle_hash": database_bundle_hash,
        "database_files": [path.as_posix() for path in database_files],
        "decoder_git_commit": decoder_git_commit,
    }


def _database_root() -> Path | None:
    mermaid_root = os.environ.get("MERMAID")
    if not mermaid_root:
        return None
    database_root = Path(mermaid_root) / "database"
    if not database_root.exists() or not database_root.is_dir():
        return None
    return database_root


def _database_files(database_root: Path | None) -> list[Path]:
    if database_root is None:
        return []
    return sorted(path for path in database_root.glob("*.json") if path.is_file())


def _bundle_hash(paths: list[Path]) -> str | None:
    if not paths:
        return None
    manifest_lines = [
        f"{path.name}\t{_hash_file(path)}"
        for path in paths
    ]
    return hashlib.sha256("\n".join(manifest_lines).encode("utf-8")).hexdigest()


def _python_version(python_executable: Path) -> str | None:
    result = subprocess.run(
        [
            str(python_executable),
            "-c",
            "import platform; print(platform.python_version())",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _git_commit(cwd: Path) -> str | None:
    result = subprocess.run(
        ["git", "-C", str(cwd), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _build_outputs_json(
    *,
    output_root: Path,
    summary: NormalizationPipelineSummary | None,
) -> dict[str, object]:
    jsonl_outputs = [
        {
            "path": path.relative_to(output_root).as_posix(),
            "size_bytes": path.stat().st_size,
        }
        for path in sorted(output_root.rglob("*.jsonl"))
        if path.is_file()
    ]
    payload: dict[str, object] = {"jsonl_outputs": jsonl_outputs}
    if summary is not None:
        payload["counts"] = {
            "log_operational_records": summary.log_summary.operational_records,
            "log_acquisition_records": summary.log_summary.acquisition_records,
            "log_ascent_request_records": summary.log_summary.ascent_request_records,
            "log_gps_records": summary.log_summary.gps_records,
            "log_transmission_records": summary.log_summary.transmission_records,
            "log_measurement_records": summary.log_summary.measurement_records,
            "log_unclassified_records": summary.log_summary.unclassified_records,
            "mer_environment_records": summary.mer_summary.environment_records,
            "mer_parameter_records": summary.mer_summary.parameter_records,
            "mer_data_records": summary.mer_summary.data_records,
        }
    return payload


def _run_status(output_root: Path, error: BaseException | None) -> str:
    if error is None:
        return "success"
    if any(output_root.rglob("*.jsonl")) or (output_root / "preflight_status.json").exists():
        return "partial"
    return "failed"


def _source_kind(path: Path) -> str:
    suffix = path.suffix.upper()
    if suffix == ".BIN":
        return "bin"
    if suffix == ".LOG":
        return "log"
    if suffix == ".MER":
        return "mer"
    raise ValueError(f"Unsupported raw source kind for manifest: {path}")


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
