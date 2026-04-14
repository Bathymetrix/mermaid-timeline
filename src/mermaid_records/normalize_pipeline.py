# SPDX-License-Identifier: MIT

"""End-to-end normalization pipeline helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
import shutil
import sys
import tempfile

from .bin2log import Bin2LogConfig, decode_workspace_logs, prepare_decode_workspace
from .discovery import iter_bin_files, iter_log_files, iter_mer_files
from .manifest import (
    begin_float_run,
    build_source_state,
    finalize_float_run,
    latest_source_state,
    output_dir_contains_manifests,
    record_pruned_sources,
)
from .normalize_log import OUTPUT_FILENAMES as LOG_OUTPUT_FILENAMES
from .normalize_log import write_log_jsonl_prototypes
from .normalize_mer import OUTPUT_FILENAMES as MER_OUTPUT_FILENAMES
from .normalize_mer import write_mer_jsonl_prototypes
from .parse_float_name import FloatName, float_name_from_vit_path, maybe_parse_float_name


type ExecutionMode = str


@dataclass(slots=True)
class FloatRunSummary:
    """Per-float execution summary for one normalization run."""

    float_id: str
    mode: str
    output_dir: str
    bin_count: int
    log_count: int
    mer_count: int
    log_action: str
    mer_action: str
    decoder_state_invalidated: bool


@dataclass(slots=True)
class PlannedFloatRun:
    """Shared per-float plan used for real runs and dry runs."""

    summary: FloatRunSummary
    current_sources: list[Path]
    float_output_dir: Path
    log_diff: dict[str, list[dict[str, object]]]
    mer_diff: dict[str, list[dict[str, object]]]
    input_file_diffs: list[dict[str, object]]


@dataclass(slots=True)
class NormalizationPipelineSummary:
    """Aggregate summary for one normalization pipeline run."""

    mode: str
    input_root: str | None
    input_files: list[str]
    output_dir: str
    processed_floats: list[FloatRunSummary]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable summary payload."""

        return asdict(self)


@dataclass(slots=True)
class DryRunSummary:
    """Aggregate dry-run planning summary."""

    mode: str
    input_root: str | None
    input_files: list[str]
    output_dir: str
    floats: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable summary payload."""

        return asdict(self)


def run_normalization_pipeline(
    input_root: Path | None = None,
    *,
    output_dir: Path,
    config: Bin2LogConfig | None = None,
    input_files: list[Path] | None = None,
    dry_run: bool = False,
) -> NormalizationPipelineSummary | DryRunSummary:
    """Run the normalization pipeline in stateful or stateless mode."""

    mode = _detect_mode(input_root=input_root, input_files=input_files)
    if mode == "stateful":
        assert input_root is not None
        return _run_stateful(
            input_root=input_root,
            output_dir=output_dir,
            config=config,
            dry_run=dry_run,
        )
    assert input_files is not None
    return _run_stateless(
        input_files=input_files,
        output_dir=output_dir,
        config=config,
        dry_run=dry_run,
    )


def _run_stateful(
    *,
    input_root: Path,
    output_dir: Path,
    config: Bin2LogConfig | None,
    dry_run: bool,
) -> NormalizationPipelineSummary | DryRunSummary:
    serial_map = _float_names_from_vit(input_root)
    grouped_sources = _group_paths(
        [*sorted(iter_bin_files(input_root)), *sorted(iter_log_files(input_root)), *sorted(iter_mer_files(input_root))]
    )
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
    previous_outputs = _previous_outputs_by_float_id(output_dir) if output_dir.exists() else {}
    all_group_keys = sorted(set(grouped_sources) | set(previous_outputs))
    normalization_version = _normalization_version()
    planned_floats: list[PlannedFloatRun] = []

    for group_key in all_group_keys:
        current_sources = grouped_sources.get(group_key, [])
        if any(path.suffix.upper() == ".BIN" for path in current_sources) and config is None:
            raise ValueError("decoder config is required when BIN inputs are present")

        previous_output_dir = previous_outputs.get(group_key)
        float_name = _resolve_float_name(
            group_key=group_key,
            paths=current_sources,
            input_root=input_root,
            previous_output_dir=previous_output_dir,
            serial_map=serial_map,
        )
        canonical_float_id = _canonical_float_id(group_key=group_key, float_name=float_name)
        float_output_name = _float_output_name(
            group_key=group_key,
            previous_output_dir=previous_output_dir,
            float_name=float_name,
        )
        float_output_dir = output_dir / float_output_name
        if not dry_run:
            _migrate_output_dir(previous_output_dir, float_output_dir)
        previous_state = latest_source_state(float_output_dir)
        current_state = build_source_state(
            raw_source_paths=current_sources,
            config=config if _has_kind(current_sources, "bin") else None,
            input_root=input_root,
            normalization_version=normalization_version,
        )
        decoder_state_invalidated = _decoder_state_invalidated(previous_state, current_state)
        input_file_diffs = _diff_sources(
            previous_state,
            current_state,
            {"bin", "log", "mer"},
            float_id=canonical_float_id,
            run_id=None,
            decoder_state_changed=decoder_state_invalidated,
        )
        log_diff = _select_diff_rows(input_file_diffs, {"bin", "log"})
        mer_diff = _select_diff_rows(input_file_diffs, {"mer"})
        summary = FloatRunSummary(
            float_id=canonical_float_id,
            mode="stateful",
            output_dir=float_output_dir.as_posix(),
            bin_count=_count_kind(current_sources, "bin"),
            log_count=_count_kind(current_sources, "log"),
            mer_count=_count_kind(current_sources, "mer"),
            log_action=_determine_action(
                diff=log_diff,
                invalidate=_general_invalidation(previous_state, current_state) or (
                    decoder_state_invalidated and _bin_dependent(previous_state, current_state)
                ),
            ),
            mer_action=_determine_action(
                diff=mer_diff,
                invalidate=_general_invalidation(previous_state, current_state),
            ),
            decoder_state_invalidated=decoder_state_invalidated,
        )
        planned_floats.append(
            PlannedFloatRun(
                summary=summary,
                current_sources=current_sources,
                float_output_dir=float_output_dir,
                log_diff=log_diff,
                mer_diff=mer_diff,
                input_file_diffs=input_file_diffs,
            )
        )

    if dry_run:
        return DryRunSummary(
            mode="stateful",
            input_root=input_root.as_posix(),
            input_files=[],
            output_dir=output_dir.as_posix(),
            floats=[_dry_run_float_payload(plan) for plan in planned_floats],
        )

    processed_floats: list[FloatRunSummary] = []
    for plan in planned_floats:
        current_sources = plan.current_sources
        summary = plan.summary

        record_pruned_sources(
            float_output_dir=plan.float_output_dir,
            float_id=summary.float_id,
            removed_sources=plan.log_diff["removed"] + plan.mer_diff["removed"],
        )

        run_context = begin_float_run(
            float_output_dir=plan.float_output_dir,
            input_root=input_root,
            raw_source_paths=current_sources,
            config=config if _has_kind(current_sources, "bin") else None,
            normalization_version=normalization_version,
        )
        run_id = str(run_context["run_id"])
        input_file_diffs = [{**row, "run_id": run_id} for row in plan.input_file_diffs]

        error: BaseException | None = None
        try:
            _execute_log_family(
                float_output_dir=plan.float_output_dir,
                action=summary.log_action,
                log_paths=_rewrite_paths(summary.log_action, current_sources, plan.log_diff, "log"),
                bin_paths=_rewrite_paths(summary.log_action, current_sources, plan.log_diff, "bin"),
                config=config,
                float_id=summary.float_id,
            )
            _execute_mer_family(
                float_output_dir=plan.float_output_dir,
                action=summary.mer_action,
                mer_paths=_rewrite_paths(summary.mer_action, current_sources, plan.mer_diff, "mer"),
                float_id=summary.float_id,
            )
        except BaseException as exc:
            error = exc
            raise
        finally:
            finalize_float_run(
                context=run_context,
                preflight_mode=config.preflight_mode if config is not None and _has_kind(current_sources, "bin") else None,
                error=error,
                input_file_diffs=_public_diff_rows(input_file_diffs),
            )

        processed_floats.append(summary)

    return NormalizationPipelineSummary(
        mode="stateful",
        input_root=input_root.as_posix(),
        input_files=[],
        output_dir=output_dir.as_posix(),
        processed_floats=processed_floats,
    )


def _run_stateless(
    *,
    input_files: list[Path],
    output_dir: Path,
    config: Bin2LogConfig | None,
    dry_run: bool,
) -> NormalizationPipelineSummary | DryRunSummary:
    if output_dir_contains_manifests(output_dir):
        raise ValueError(
            "stateless mode cannot write into an output directory that already contains manifests"
        )

    grouped_sources = _group_paths(input_files)
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
    normalization_version = _normalization_version()
    processed_floats: list[FloatRunSummary] = []
    dry_run_floats: list[dict[str, object]] = []

    for group_key, current_sources in sorted(grouped_sources.items()):
        if any(path.suffix.upper() == ".BIN" for path in current_sources) and config is None:
            raise ValueError("decoder config is required when BIN inputs are present")
        float_name = _resolve_float_name(
            group_key=group_key,
            paths=current_sources,
            input_root=None,
            previous_output_dir=None,
            serial_map={},
        )
        canonical_float_id = _canonical_float_id(group_key=group_key, float_name=float_name)
        float_output_dir = output_dir / _float_output_name(
            group_key=group_key,
            previous_output_dir=None,
            float_name=float_name,
        )
        input_file_diffs = _diff_sources(
            None,
            build_source_state(
                raw_source_paths=current_sources,
                config=config if _has_kind(current_sources, "bin") else None,
                input_root=Path("."),
                normalization_version=normalization_version,
            ),
            {"bin", "log", "mer"},
            float_id=canonical_float_id,
            run_id=None,
            decoder_state_changed=False,
        )
        summary = FloatRunSummary(
            float_id=canonical_float_id,
            mode="stateless",
            output_dir=float_output_dir.as_posix(),
            bin_count=_count_kind(current_sources, "bin"),
            log_count=_count_kind(current_sources, "log"),
            mer_count=_count_kind(current_sources, "mer"),
            log_action="append" if _has_kind(current_sources, "bin") or _has_kind(current_sources, "log") else "noop",
            mer_action="append" if _has_kind(current_sources, "mer") else "noop",
            decoder_state_invalidated=False,
        )
        if dry_run:
            dry_run_floats.append(
                _dry_run_float_payload(
                    PlannedFloatRun(
                        summary=summary,
                        current_sources=current_sources,
                        float_output_dir=float_output_dir,
                        log_diff=_select_diff_rows(input_file_diffs, {"bin", "log"}),
                        mer_diff=_select_diff_rows(input_file_diffs, {"mer"}),
                        input_file_diffs=input_file_diffs,
                    )
                )
            )
            continue

        _execute_log_family(
            float_output_dir=float_output_dir,
            action="append",
            log_paths=_selected_paths(current_sources, {"log"}),
            bin_paths=_selected_paths(current_sources, {"bin"}),
            config=config,
            float_id=summary.float_id,
        )
        _execute_mer_family(
            float_output_dir=float_output_dir,
            action="append",
            mer_paths=_selected_paths(current_sources, {"mer"}),
            float_id=summary.float_id,
        )
        processed_floats.append(summary)

    if dry_run:
        return DryRunSummary(
            mode="stateless",
            input_root=None,
            input_files=[path.as_posix() for path in sorted(input_files)],
            output_dir=output_dir.as_posix(),
            floats=dry_run_floats,
        )

    return NormalizationPipelineSummary(
        mode="stateless",
        input_root=None,
        input_files=[path.as_posix() for path in sorted(input_files)],
        output_dir=output_dir.as_posix(),
        processed_floats=processed_floats,
    )


def _execute_log_family(
    *,
    float_output_dir: Path,
    action: str,
    log_paths: list[Path],
    bin_paths: list[Path],
    config: Bin2LogConfig | None,
    float_id: str,
) -> None:
    destinations = [float_output_dir / filename for filename in LOG_OUTPUT_FILENAMES.values()]
    if action == "noop":
        return
    if action == "rewrite" and not log_paths and not bin_paths:
        _remove_paths(destinations)
        return
    if not log_paths and not bin_paths:
        return
    if bin_paths and config is None:
        raise ValueError("decoder config is required when BIN inputs are present")

    float_output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mermaid-log-family-") as tmpdir:
        temp_dir = Path(tmpdir)
        rendered_paths = list(log_paths)
        if bin_paths:
            assert config is not None
            decode_config = Bin2LogConfig(
                python_executable=config.python_executable,
                decoder_script=config.decoder_script,
                preflight_mode=config.preflight_mode,
                preflight_status_dir=float_output_dir,
            )
            workdir = temp_dir / "decoded"
            workdir.mkdir(parents=True, exist_ok=True)
            for path in bin_paths:
                shutil.copy2(path, workdir / path.name)
            prepare_decode_workspace(workdir, config=decode_config, refresh_database=True)
            rendered_paths.extend(decode_workspace_logs(workdir, config=decode_config))
        write_log_jsonl_prototypes(rendered_paths, temp_dir, float_id=float_id)
        if action == "append":
            _append_rendered_outputs(
                temp_dir=temp_dir,
                destination_dir=float_output_dir,
                filenames=list(LOG_OUTPUT_FILENAMES.values()),
            )
        else:
            _replace_rendered_outputs(
                temp_dir=temp_dir,
                destination_dir=float_output_dir,
                filenames=list(LOG_OUTPUT_FILENAMES.values()),
            )


def _execute_mer_family(
    *,
    float_output_dir: Path,
    action: str,
    mer_paths: list[Path],
    float_id: str,
) -> None:
    destinations = [float_output_dir / filename for filename in MER_OUTPUT_FILENAMES.values()]
    if action == "noop":
        return
    if action == "rewrite" and not mer_paths:
        _remove_paths(destinations)
        return
    if not mer_paths:
        return

    float_output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mermaid-mer-family-") as tmpdir:
        temp_dir = Path(tmpdir)
        write_mer_jsonl_prototypes(mer_paths, temp_dir, float_id=float_id)
        if action == "append":
            _append_rendered_outputs(
                temp_dir=temp_dir,
                destination_dir=float_output_dir,
                filenames=list(MER_OUTPUT_FILENAMES.values()),
            )
        else:
            _replace_rendered_outputs(
                temp_dir=temp_dir,
                destination_dir=float_output_dir,
                filenames=list(MER_OUTPUT_FILENAMES.values()),
            )


def _append_rendered_outputs(
    *,
    temp_dir: Path,
    destination_dir: Path,
    filenames: list[str],
) -> None:
    for filename in filenames:
        source = temp_dir / filename
        if not source.exists() or source.stat().st_size == 0:
            continue
        destination = destination_dir / filename
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("a", encoding="utf-8") as dst_handle:
            dst_handle.write(source.read_text(encoding="utf-8"))


def _replace_rendered_outputs(
    *,
    temp_dir: Path,
    destination_dir: Path,
    filenames: list[str],
) -> None:
    destination_dir.mkdir(parents=True, exist_ok=True)
    for filename in filenames:
        source = temp_dir / filename
        destination = destination_dir / filename
        if not source.exists():
            if destination.exists():
                destination.unlink()
            continue
        shutil.copyfile(source, destination)


def _remove_paths(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def _detect_mode(
    *,
    input_root: Path | None,
    input_files: list[Path] | None,
) -> ExecutionMode:
    has_root = input_root is not None
    has_files = bool(input_files)
    if has_root == has_files:
        raise ValueError("provide exactly one of input_root or input_files")
    return "stateful" if has_root else "stateless"


def _group_paths(paths: list[Path]) -> dict[str, list[Path]]:
    grouped: dict[str, list[Path]] = {}
    for path in sorted(paths):
        grouped.setdefault(_raw_file_prefix(path), []).append(path)
    return grouped


def _raw_file_prefix(path: Path) -> str:
    return path.stem.split("_", maxsplit=1)[0]


def _float_output_name(
    *,
    group_key: str,
    previous_output_dir: Path | None,
    float_name: FloatName | None,
) -> str:
    if float_name is not None:
        return float_name.serial
    if previous_output_dir is not None and _looks_like_full_serial(previous_output_dir.name):
        return previous_output_dir.name
    if previous_output_dir is not None:
        return previous_output_dir.name
    return group_key


def _float_names_from_vit(input_root: Path) -> dict[str, FloatName]:
    serial_map: dict[str, FloatName] = {}
    for path in sorted(input_root.glob("*.vit")) + sorted(input_root.glob("*.VIT")):
        float_name = float_name_from_vit_path(path)
        if float_name is None:
            continue
        serial_map[float_name.raw_file_prefix] = float_name
    return serial_map


def _resolve_float_name(
    *,
    group_key: str,
    paths: list[Path],
    input_root: Path | None,
    previous_output_dir: Path | None,
    serial_map: dict[str, FloatName],
) -> FloatName | None:
    if group_key in serial_map:
        return serial_map[group_key]
    if input_root is not None:
        candidate = maybe_parse_float_name(input_root.name)
        if candidate is not None and candidate.raw_file_prefix == group_key:
            return candidate
    if previous_output_dir is not None:
        candidate = maybe_parse_float_name(previous_output_dir.name)
        if candidate is not None:
            return candidate
    for path in paths:
        for ancestor in path.parents:
            if input_root is not None and ancestor == input_root.parent:
                break
            candidate = maybe_parse_float_name(ancestor.name)
            if candidate is not None:
                return candidate
    for path in paths:
        serial = _serial_from_log(path)
        if serial is not None:
            candidate = maybe_parse_float_name(serial)
            if candidate is not None:
                return candidate
    return None


def _looks_like_full_serial(name: str) -> bool:
    return maybe_parse_float_name(name) is not None


def _canonical_float_id(*, group_key: str, float_name: FloatName | None) -> str:
    if float_name is not None:
        return float_name.float_id
    return group_key


def _serial_from_log(path: Path) -> str | None:
    if path.suffix.upper() != ".LOG" or not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for _ in range(32):
                line = handle.readline()
                if not line:
                    break
                if "]buoy " in line:
                    candidate = line.split("]buoy ", maxsplit=1)[1].strip()
                    if _looks_like_full_serial(candidate):
                        return candidate
                if "]board " in line:
                    candidate = line.split("]board ", maxsplit=1)[1].strip()
                    if _looks_like_full_serial(candidate):
                        return candidate
    except OSError:
        return None
    return None


def _previous_outputs_by_float_id(output_dir: Path) -> dict[str, Path]:
    previous: dict[str, Path] = {}
    for latest_path in output_dir.glob("*/manifests/latest.json"):
        float_output_dir = latest_path.parent.parent
        state = latest_source_state(float_output_dir)
        if state is None:
            continue
        raw_sources = state.get("raw_sources", [])
        if not raw_sources:
            continue
        previous[_raw_file_prefix(Path(raw_sources[0]["source_file"]))] = float_output_dir
    return previous


def _migrate_output_dir(previous_output_dir: Path | None, float_output_dir: Path) -> None:
    if previous_output_dir is None or previous_output_dir == float_output_dir:
        return
    if not previous_output_dir.exists() or float_output_dir.exists():
        return
    float_output_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(previous_output_dir.as_posix(), float_output_dir.as_posix())


def _selected_paths(paths: list[Path], kinds: set[str]) -> list[Path]:
    return [path for path in paths if _kind_for_path(path) in kinds]


def _kind_for_path(path: Path) -> str:
    suffix = path.suffix.upper()
    if suffix == ".BIN":
        return "bin"
    if suffix == ".LOG":
        return "log"
    if suffix == ".MER":
        return "mer"
    raise ValueError(f"Unsupported normalization source: {path}")


def _has_kind(paths: list[Path], kind: str) -> bool:
    return any(_kind_for_path(path) == kind for path in paths)


def _count_kind(paths: list[Path], kind: str) -> int:
    return sum(1 for path in paths if _kind_for_path(path) == kind)


def _paths_from_sources(sources: list[dict[str, object]]) -> list[Path]:
    return [Path(str(source.get("_source_path", source["source_file"]))) for source in sources]


def _diff_sources(
    previous_state: dict[str, object] | None,
    current_state: dict[str, object],
    kinds: set[str],
    *,
    float_id: str,
    run_id: str | None,
    decoder_state_changed: bool,
) -> list[dict[str, object]]:
    previous_sources = {
        item["source_file"]: item
        for item in (previous_state or {}).get("raw_sources", [])
        if item["source_kind"] in kinds
    }
    current_sources = {
        item["source_file"]: item
        for item in current_state["raw_sources"]
        if item["source_kind"] in kinds
    }
    rows: list[dict[str, object]] = []
    for source_file in sorted(set(previous_sources) | set(current_sources)):
        previous = previous_sources.get(source_file)
        current = current_sources.get(source_file)
        previous_exists = previous is not None
        current_exists = current is not None
        previous_size = int(previous["size_bytes"]) if previous is not None else 0
        current_size = int(current["size_bytes"]) if current is not None else 0
        previous_hash = str(previous["content_hash"]) if previous is not None else None
        current_hash = str(current["content_hash"]) if current is not None else None
        source = current or previous
        assert source is not None
        if not previous_exists:
            change_kind = "new"
        elif not current_exists:
            change_kind = "removed"
        elif previous_hash != current_hash:
            change_kind = "changed"
        else:
            change_kind = "unchanged"
        rows.append(
            {
                "source_file": Path(source_file).name,
                "_source_path": source_file,
                "source_kind": source["source_kind"],
                "float_id": float_id,
                "previous_exists": previous_exists,
                "current_exists": current_exists,
                "previous_size_bytes": previous_size,
                "current_size_bytes": current_size,
                "previous_hash": previous_hash,
                "current_hash": current_hash,
                "change_kind": change_kind,
                "decoder_state_changed": bool(decoder_state_changed and source["source_kind"] == "bin"),
                "run_id": run_id,
            }
        )
    return rows


def _select_diff_rows(
    rows: list[dict[str, object]],
    kinds: set[str],
) -> dict[str, list[dict[str, object]]]:
    selected = [row for row in rows if row["source_kind"] in kinds]
    return {
        "added": [row for row in selected if row["change_kind"] == "new"],
        "removed": [row for row in selected if row["change_kind"] == "removed"],
        "changed": [row for row in selected if row["change_kind"] == "changed"],
    }


def _determine_action(*, diff: dict[str, list[dict[str, object]]], invalidate: bool) -> str:
    if invalidate:
        return "rewrite"
    if diff["removed"] or diff["changed"]:
        return "rewrite"
    if diff["added"]:
        return "append"
    return "noop"


def _general_invalidation(
    previous_state: dict[str, object] | None,
    current_state: dict[str, object],
) -> bool:
    if previous_state is None:
        return False
    return (
        previous_state.get("input_root") != current_state.get("input_root")
        or previous_state.get("normalization_version") != current_state.get("normalization_version")
    )


def _decoder_state_invalidated(
    previous_state: dict[str, object] | None,
    current_state: dict[str, object],
) -> bool:
    if previous_state is None:
        return False
    return previous_state.get("decoder_state") != current_state.get("decoder_state")


def _bin_dependent(
    previous_state: dict[str, object] | None,
    current_state: dict[str, object],
) -> bool:
    previous_has_bin = any(
        item["source_kind"] == "bin"
        for item in (previous_state or {}).get("raw_sources", [])
    )
    current_has_bin = any(item["source_kind"] == "bin" for item in current_state["raw_sources"])
    return previous_has_bin or current_has_bin


def _normalization_version() -> str:
    package = sys.modules.get("mermaid_records")
    version = getattr(package, "__version__", None)
    if not isinstance(version, str):
        raise RuntimeError("mermaid_records.__version__ is not available")
    return version


def _rewrite_paths(
    action: str,
    current_sources: list[Path],
    diff: dict[str, list[dict[str, object]]],
    kind: str,
) -> list[Path]:
    if action == "rewrite":
        return _selected_paths(current_sources, {kind})
    return _paths_from_sources([item for item in diff["added"] if item["source_kind"] == kind])


def _dry_run_float_payload(plan: PlannedFloatRun) -> dict[str, object]:
    counts = {
        "total": len(plan.input_file_diffs),
        "new": sum(1 for row in plan.input_file_diffs if row["change_kind"] == "new"),
        "changed": sum(1 for row in plan.input_file_diffs if row["change_kind"] == "changed"),
        "removed": sum(1 for row in plan.input_file_diffs if row["change_kind"] == "removed"),
        "unchanged": sum(1 for row in plan.input_file_diffs if row["change_kind"] == "unchanged"),
    }
    return {
        "float_id": plan.summary.float_id,
        "output_dir": plan.summary.output_dir,
        "counts": counts,
        "families": {
            "log": {
                "action": plan.summary.log_action,
                "file_diffs": _changed_file_rows(plan.input_file_diffs, {"bin", "log"}),
                "decoder_invalidated": _decoder_invalidated_rows(plan.input_file_diffs, {"bin", "log"}),
            },
            "mer": {
                "action": plan.summary.mer_action,
                "file_diffs": _changed_file_rows(plan.input_file_diffs, {"mer"}),
                "decoder_invalidated": [],
            },
        },
    }


def _changed_file_rows(
    rows: list[dict[str, object]],
    kinds: set[str],
) -> list[dict[str, object]]:
    return [
        _public_diff_row(row)
        for row in rows
        if row["source_kind"] in kinds and row["change_kind"] in {"new", "changed", "removed"}
    ]


def _decoder_invalidated_rows(
    rows: list[dict[str, object]],
    kinds: set[str],
) -> list[dict[str, object]]:
    return [
        _public_diff_row(row)
        for row in rows
        if row["source_kind"] in kinds
        and row["decoder_state_changed"]
        and row["change_kind"] == "unchanged"
    ]


def _public_diff_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [_public_diff_row(row) for row in rows]


def _public_diff_row(row: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in row.items() if key != "_source_path"}
