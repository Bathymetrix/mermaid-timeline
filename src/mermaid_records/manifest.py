# SPDX-License-Identifier: MIT

"""Per-float manifest persistence for normalization pipeline runs."""

from __future__ import annotations

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


def begin_float_run(
    *,
    float_output_dir: Path,
    input_root: Path,
    raw_source_paths: list[Path],
    config: Bin2LogConfig | None,
    normalization_version: str,
) -> dict[str, object]:
    """Create manifest context for one float-level stateful run."""

    started_at = _iso_now()
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    manifests_root = float_output_dir / "manifests"
    run_dir = manifests_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    source_state = build_source_state(
        raw_source_paths=raw_source_paths,
        config=config if _has_bin(raw_source_paths) else None,
        input_root=input_root,
        normalization_version=normalization_version,
    )
    return {
        "run_id": run_id,
        "started_at": started_at,
        "run_dir": run_dir,
        "manifests_root": manifests_root,
        "float_output_dir": float_output_dir,
        "source_state": source_state,
    }


def finalize_float_run(
    *,
    context: dict[str, object],
    preflight_mode: str | None,
    error: BaseException | None,
    input_file_diffs: list[dict[str, object]] | None = None,
) -> None:
    """Write per-float manifests for a completed or failed run."""

    run_dir = Path(context["run_dir"])
    manifests_root = Path(context["manifests_root"])
    float_output_dir = Path(context["float_output_dir"])
    source_state = context["source_state"]
    run_json = {
        "run_id": context["run_id"],
        "started_at": context["started_at"],
        "completed_at": _iso_now(),
        "input_root": source_state["input_root"],
        "output_root": float_output_dir.as_posix(),
        "normalization_version": source_state["normalization_version"],
        "preflight_mode": preflight_mode,
        "status": _run_status(float_output_dir, error),
    }
    outputs_json = build_outputs_manifest(float_output_dir)

    _write_json(run_dir / "run.json", run_json)
    _write_json(run_dir / "outputs.json", outputs_json)
    _write_json(run_dir / "source_state.json", source_state)
    _write_jsonl(run_dir / "input_file_diffs.jsonl", input_file_diffs or [])

    preflight_root = float_output_dir / "preflight_status.json"
    if preflight_root.exists():
        (run_dir / "preflight_status.json").write_text(
            preflight_root.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    latest_json = {
        "run_id": context["run_id"],
        "status": run_json["status"],
        "started_at": context["started_at"],
        "completed_at": run_json["completed_at"],
        "run_manifest": (run_dir / "run.json").relative_to(float_output_dir).as_posix(),
        "outputs_manifest": (run_dir / "outputs.json").relative_to(float_output_dir).as_posix(),
        "source_state_manifest": (
            (run_dir / "source_state.json").relative_to(float_output_dir).as_posix()
        ),
        "preflight_status": (
            (run_dir / "preflight_status.json").relative_to(float_output_dir).as_posix()
            if (run_dir / "preflight_status.json").exists()
            else None
        ),
    }
    manifests_root.mkdir(parents=True, exist_ok=True)
    _write_json(manifests_root / "latest.json", latest_json)


def latest_source_state(float_output_dir: Path) -> dict[str, object] | None:
    """Load the latest persisted source state for one float, if present."""

    latest_path = float_output_dir / "manifests" / "latest.json"
    if not latest_path.exists():
        return None
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    source_state_path = float_output_dir / latest["source_state_manifest"]
    if not source_state_path.exists():
        return None
    return json.loads(source_state_path.read_text(encoding="utf-8"))


def output_dir_contains_manifests(output_dir: Path) -> bool:
    """Return whether the output tree already contains manifests."""

    if not output_dir.exists():
        return False
    return any(path.is_dir() and path.name == "manifests" for path in output_dir.rglob("manifests"))


def build_source_state(
    *,
    raw_source_paths: list[Path],
    config: Bin2LogConfig | None,
    input_root: Path,
    normalization_version: str,
) -> dict[str, object]:
    """Build source state for one float-level run."""

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
        "input_root": input_root.as_posix(),
        "normalization_version": normalization_version,
        "raw_sources": raw_sources,
        "decoder_state": _decoder_state(config),
    }


def build_outputs_manifest(float_output_dir: Path) -> dict[str, object]:
    """Build the output inventory for one float-level output root."""

    jsonl_outputs = [
        {
            "path": path.relative_to(float_output_dir).as_posix(),
            "size_bytes": path.stat().st_size,
        }
        for path in sorted(float_output_dir.glob("*.jsonl"))
        if path.is_file()
    ]
    counts = {}
    for item in jsonl_outputs:
        path = float_output_dir / item["path"]
        with path.open("r", encoding="utf-8") as handle:
            counts[path.name.removesuffix(".jsonl")] = sum(1 for line in handle if line.strip())
    return {
        "jsonl_outputs": jsonl_outputs,
        "counts": counts,
    }


def record_pruned_sources(
    *,
    float_output_dir: Path,
    float_id: str,
    removed_sources: list[dict[str, object]],
) -> None:
    """Append pruned-source records for removed raw files."""

    if not removed_sources:
        return
    state_dir = float_output_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    pruned_path = state_dir / "pruned_records.jsonl"
    removed_at = _iso_now()
    with pruned_path.open("a", encoding="utf-8") as handle:
        for source in removed_sources:
            record = {
                "source_file": source.get("_source_path", source["source_file"]),
                "source_kind": source["source_kind"],
                "float_id": float_id,
                "removed_at": removed_at,
            }
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")


def _decoder_state(config: Bin2LogConfig | None) -> dict[str, object] | None:
    if config is None:
        return None

    database_root = _database_root()
    database_files = _database_files(database_root)
    return {
        "decoder_python": str(config.python_executable),
        "decoder_python_version": _python_version(config.python_executable),
        "decoder_script": str(config.decoder_script),
        "decoder_script_hash": _hash_file(config.decoder_script),
        "preflight_mode": config.preflight_mode,
        "database_bundle_hash": _bundle_hash(database_files),
        "database_files": [path.as_posix() for path in database_files],
        "decoder_git_commit": _git_commit(config.decoder_script.parent),
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
    manifest_lines = [f"{path.name}\t{_hash_file(path)}" for path in paths]
    return hashlib.sha256("\n".join(manifest_lines).encode("utf-8")).hexdigest()


def _python_version(python_executable: Path) -> str | None:
    result = subprocess.run(
        [str(python_executable), "-c", "import platform; print(platform.python_version())"],
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


def _run_status(float_output_dir: Path, error: BaseException | None) -> str:
    if error is None:
        return "success"
    if any(float_output_dir.glob("*.jsonl")) or (float_output_dir / "preflight_status.json").exists():
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


def _has_bin(paths: list[Path]) -> bool:
    return any(path.suffix.upper() == ".BIN" for path in paths)


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


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
