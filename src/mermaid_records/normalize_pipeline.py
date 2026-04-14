# SPDX-License-Identifier: MIT

"""End-to-end normalization pipeline helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import shutil
import sys
import tempfile

from .bin2log import Bin2LogConfig, decode_workspace_logs, prepare_decode_workspace
from .discovery import iter_bin_files, iter_log_files, iter_mer_files
from .manifest import (
    begin_float_run,
    build_outputs_manifest,
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


def run_normalization_pipeline(
    input_root: Path | None = None,
    *,
    output_dir: Path,
    config: Bin2LogConfig | None = None,
    input_files: list[Path] | None = None,
) -> NormalizationPipelineSummary:
    """Run the normalization pipeline in stateful or stateless mode."""

    mode = _detect_mode(input_root=input_root, input_files=input_files)
    if mode == "stateful":
        assert input_root is not None
        return _run_stateful(input_root=input_root, output_dir=output_dir, config=config)
    assert input_files is not None
    return _run_stateless(input_files=input_files, output_dir=output_dir, config=config)


def _run_stateful(
    *,
    input_root: Path,
    output_dir: Path,
    config: Bin2LogConfig | None,
) -> NormalizationPipelineSummary:
    grouped_sources = _group_paths(
        [*sorted(iter_bin_files(input_root)), *sorted(iter_log_files(input_root)), *sorted(iter_mer_files(input_root))]
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    previous_float_ids = {
        path.parent.parent.name for path in output_dir.glob("*/manifests/latest.json")
    }
    all_float_ids = sorted(set(grouped_sources) | previous_float_ids)
    processed_floats: list[FloatRunSummary] = []
    normalization_version = _normalization_version()

    for float_id in all_float_ids:
        current_sources = grouped_sources.get(float_id, [])
        if any(path.suffix.upper() == ".BIN" for path in current_sources) and config is None:
            raise ValueError("decoder config is required when BIN inputs are present")

        float_output_dir = output_dir / float_id
        previous_state = latest_source_state(float_output_dir)
        current_state = build_source_state(
            raw_source_paths=current_sources,
            config=config if _has_kind(current_sources, "bin") else None,
            input_root=input_root,
            normalization_version=normalization_version,
        )
        log_diff = _diff_sources(previous_state, current_state, {"bin", "log"})
        mer_diff = _diff_sources(previous_state, current_state, {"mer"})
        decoder_state_invalidated = _decoder_state_invalidated(previous_state, current_state)
        log_action = _determine_action(
            diff=log_diff,
            invalidate=_general_invalidation(previous_state, current_state) or (
                decoder_state_invalidated and _bin_dependent(previous_state, current_state)
            ),
        )
        mer_action = _determine_action(
            diff=mer_diff,
            invalidate=_general_invalidation(previous_state, current_state),
        )

        record_pruned_sources(
            float_output_dir=float_output_dir,
            float_id=float_id,
            removed_sources=log_diff["removed"] + mer_diff["removed"],
        )

        run_context = begin_float_run(
            float_output_dir=float_output_dir,
            input_root=input_root,
            raw_source_paths=current_sources,
            config=config if _has_kind(current_sources, "bin") else None,
            normalization_version=normalization_version,
        )
        error: BaseException | None = None
        try:
            _execute_log_family(
                float_output_dir=float_output_dir,
                action=log_action,
                log_paths=_selected_paths(current_sources, {"log"}) if log_action == "rewrite" else _paths_from_sources(
                    [item for item in log_diff["added"] if item["source_kind"] == "log"]
                ),
                bin_paths=_selected_paths(current_sources, {"bin"}) if log_action == "rewrite" else _paths_from_sources(
                    [item for item in log_diff["added"] if item["source_kind"] == "bin"]
                ),
                config=config,
            )
            _execute_mer_family(
                float_output_dir=float_output_dir,
                action=mer_action,
                mer_paths=_selected_paths(current_sources, {"mer"}) if mer_action == "rewrite" else _paths_from_sources(mer_diff["added"]),
            )
        except BaseException as exc:
            error = exc
            raise
        finally:
            finalize_float_run(
                context=run_context,
                preflight_mode=config.preflight_mode if config is not None and _has_kind(current_sources, "bin") else None,
                error=error,
            )

        processed_floats.append(
            FloatRunSummary(
                float_id=float_id,
                mode="stateful",
                output_dir=float_output_dir.as_posix(),
                bin_count=_count_kind(current_sources, "bin"),
                log_count=_count_kind(current_sources, "log"),
                mer_count=_count_kind(current_sources, "mer"),
                log_action=log_action,
                mer_action=mer_action,
                decoder_state_invalidated=decoder_state_invalidated,
            )
        )

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
) -> NormalizationPipelineSummary:
    if output_dir_contains_manifests(output_dir):
        raise ValueError(
            "stateless mode cannot write into an output directory that already contains manifests"
        )

    grouped_sources = _group_paths(input_files)
    output_dir.mkdir(parents=True, exist_ok=True)
    processed_floats: list[FloatRunSummary] = []

    for float_id, current_sources in sorted(grouped_sources.items()):
        if any(path.suffix.upper() == ".BIN" for path in current_sources) and config is None:
            raise ValueError("decoder config is required when BIN inputs are present")
        float_output_dir = output_dir / float_id
        _execute_log_family(
            float_output_dir=float_output_dir,
            action="append",
            log_paths=_selected_paths(current_sources, {"log"}),
            bin_paths=_selected_paths(current_sources, {"bin"}),
            config=config,
        )
        _execute_mer_family(
            float_output_dir=float_output_dir,
            action="append",
            mer_paths=_selected_paths(current_sources, {"mer"}),
        )
        processed_floats.append(
            FloatRunSummary(
                float_id=float_id,
                mode="stateless",
                output_dir=float_output_dir.as_posix(),
                bin_count=_count_kind(current_sources, "bin"),
                log_count=_count_kind(current_sources, "log"),
                mer_count=_count_kind(current_sources, "mer"),
                log_action="append" if _has_kind(current_sources, "bin") or _has_kind(current_sources, "log") else "noop",
                mer_action="append" if _has_kind(current_sources, "mer") else "noop",
                decoder_state_invalidated=False,
            )
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
        write_log_jsonl_prototypes(rendered_paths, temp_dir)
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
        write_mer_jsonl_prototypes(mer_paths, temp_dir)
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
        grouped.setdefault(_float_id(path), []).append(path)
    return grouped


def _float_id(path: Path) -> str:
    return path.stem.split("_", maxsplit=1)[0]


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
    return [Path(source["source_file"]) for source in sources]


def _diff_sources(
    previous_state: dict[str, object] | None,
    current_state: dict[str, object],
    kinds: set[str],
) -> dict[str, list[dict[str, object]]]:
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
    added = [current_sources[path] for path in sorted(set(current_sources) - set(previous_sources))]
    removed = [previous_sources[path] for path in sorted(set(previous_sources) - set(current_sources))]
    changed = [
        current_sources[path]
        for path in sorted(set(previous_sources) & set(current_sources))
        if previous_sources[path]["content_hash"] != current_sources[path]["content_hash"]
    ]
    return {"added": added, "removed": removed, "changed": changed}


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
