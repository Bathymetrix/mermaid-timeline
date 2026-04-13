# SPDX-License-Identifier: MIT

"""End-to-end normalization pipeline helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from pathlib import Path
import shutil
import sys
import tempfile

from .bin2log import Bin2LogConfig, decode_workspace_logs, prepare_decode_workspace
from .discovery import iter_bin_files, iter_log_files, iter_mer_files
from .manifest import begin_run, finalize_run
from .normalize_log import LogJsonlPrototypeSummary, write_log_jsonl_prototypes
from .normalize_mer import MerJsonlPrototypeSummary, write_mer_jsonl_prototypes


@dataclass(slots=True)
class NormalizationPipelineSummary:
    """Aggregate summary for one normalization pipeline run."""

    input_root: str
    output_dir: str
    bin_count: int
    log_count: int
    mer_count: int
    decoded_log_count: int
    log_jsonl_dir: str
    mer_jsonl_dir: str
    log_summary: LogJsonlPrototypeSummary
    mer_summary: MerJsonlPrototypeSummary

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable summary payload."""

        payload = asdict(self)
        payload["log_summary"] = asdict(self.log_summary)
        payload["mer_summary"] = asdict(self.mer_summary)
        return payload


def run_normalization_pipeline(
    input_root: Path,
    *,
    output_dir: Path,
    config: Bin2LogConfig | None = None,
) -> NormalizationPipelineSummary:
    """Run the package normalization pipeline from raw inputs to JSONL outputs."""

    bin_paths = sorted(iter_bin_files(input_root))
    discovered_log_paths = sorted(iter_log_files(input_root))
    mer_paths = sorted(iter_mer_files(input_root))
    raw_source_paths = [*bin_paths, *discovered_log_paths, *mer_paths]

    if bin_paths and config is None:
        raise ValueError("decoder config is required when BIN inputs are present")

    output_dir.mkdir(parents=True, exist_ok=True)
    run_context = begin_run(
        input_root=input_root,
        output_root=output_dir,
        config=config,
        raw_source_paths=raw_source_paths,
    )

    decoded_log_paths: list[Path] = []
    summary: NormalizationPipelineSummary | None = None
    error: BaseException | None = None
    try:
        if bin_paths:
            assert config is not None
            pipeline_config = replace(config, preflight_status_dir=output_dir)
            with tempfile.TemporaryDirectory(prefix="mermaid-normalize-decode-") as tmpdir:
                workdir = Path(tmpdir)
                for path in bin_paths:
                    shutil.copy2(path, workdir / path.name)
                prepare_decode_workspace(workdir, config=pipeline_config, refresh_database=True)
                decoded_log_paths = decode_workspace_logs(workdir, config=pipeline_config)
                log_paths = sorted(
                    {path.resolve(): path for path in [*discovered_log_paths, *decoded_log_paths]}.values()
                )
                log_summary = write_log_jsonl_prototypes(log_paths, output_dir / "log_jsonl")
        else:
            log_paths = discovered_log_paths
            log_summary = write_log_jsonl_prototypes(log_paths, output_dir / "log_jsonl")

        mer_summary = write_mer_jsonl_prototypes(mer_paths, output_dir / "mer_jsonl")

        summary = NormalizationPipelineSummary(
            input_root=input_root.as_posix(),
            output_dir=output_dir.as_posix(),
            bin_count=len(bin_paths),
            log_count=len(discovered_log_paths),
            mer_count=len(mer_paths),
            decoded_log_count=len(decoded_log_paths),
            log_jsonl_dir=(output_dir / "log_jsonl").as_posix(),
            mer_jsonl_dir=(output_dir / "mer_jsonl").as_posix(),
            log_summary=log_summary,
            mer_summary=mer_summary,
        )
        return summary
    except BaseException as exc:
        error = exc
        raise
    finally:
        finalize_run(
            context=run_context,
            normalization_version=_normalization_version(),
            preflight_mode=config.preflight_mode if config is not None else None,
            summary=summary,
            error=error,
        )


def _normalization_version() -> str:
    """Read the package version from the already-loaded package module."""

    package = sys.modules.get("mermaid_records")
    version = getattr(package, "__version__", None)
    if not isinstance(version, str):
        raise RuntimeError("mermaid_records.__version__ is not available")
    return version
