# SPDX-License-Identifier: MIT

"""Audit helper for exercising the normalize CLI across a flag matrix."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
from typing import Literal


RAW_SUFFIXES = {".BIN", ".LOG", ".MER"}
INPUT_ROOT_MODE = "input_root"
INPUT_FILE_MODE = "input_file"


@dataclass(frozen=True, slots=True)
class DiscoveredInputs:
    """Raw source inventory used to build run scenarios."""

    input_root: Path
    raw_files: tuple[Path, ...]
    bin_files: tuple[Path, ...]
    log_files: tuple[Path, ...]
    mer_files: tuple[Path, ...]

    @property
    def has_bin(self) -> bool:
        return bool(self.bin_files)

    def counts(self) -> dict[str, int]:
        """Return a compact per-suffix inventory."""

        return {
            "raw_files": len(self.raw_files),
            "bin_files": len(self.bin_files),
            "log_files": len(self.log_files),
            "mer_files": len(self.mer_files),
        }


@dataclass(frozen=True, slots=True)
class InputScenario:
    """One way of selecting sources for the CLI."""

    name: str
    input_mode: Literal["input_root", "input_file"]
    input_root: Path
    input_files: tuple[Path, ...]
    has_bin: bool
    manifest_path: Path | None


@dataclass(frozen=True, slots=True)
class DecoderConfigChoice:
    """CLI/env decoder resolution variant."""

    name: str
    cli_python: Path | None
    cli_script: Path | None
    env_python: Path | None
    env_script: Path | None
    available: bool
    note: str | None


@dataclass(frozen=True, slots=True)
class RunSpec:
    """One concrete normalize invocation."""

    run_id: str
    slug: str
    input_scenario: str
    input_mode: Literal["input_root", "input_file"]
    output_mode: Literal["cli_arg", "mermaid_env", "missing"]
    preflight_mode: Literal["strict", "cached"]
    dry_run: bool
    force_rewrite: bool
    json_output: bool
    verbose: bool
    decoder_mode: str
    expects_success: bool
    availability_issue: str | None
    command: tuple[str, ...]
    output_dir: Path | None
    artifacts_dir: Path
    env_overrides: dict[str, str | None]
    input_manifest_path: Path | None
    has_bin: bool
    decoder_mermaid_root: Path | None
    seed_decoder_database: bool


@dataclass(frozen=True, slots=True)
class RunResult:
    """Recorded outcome for one attempted or skipped run."""

    run_id: str
    slug: str
    status: str
    returncode: int | None
    duration_s: float
    started_at: str
    completed_at: str
    stdout_path: str | None
    stderr_path: str | None
    stdout_preview: str
    stderr_preview: str
    exception: str | None
    summary: str


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the audit utility argument parser."""

    parser = argparse.ArgumentParser(
        description=(
            "Exercise the normalize CLI across stateful/stateless, output-resolution, "
            "decoder-resolution, and optional-flag combinations without stopping on failures."
        )
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        required=True,
        help="Root directory to scan for raw BIN/LOG/MER files.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        required=True,
        help="Directory that will receive audit artifacts, reports, and per-run output sandboxes.",
    )
    parser.add_argument(
        "--cli-command",
        default=f"{shlex.quote(sys.executable)} -m mermaid_records.cli",
        help=(
            "Base command used to invoke the CLI before the fixed normalize subcommand. "
            "Examples: 'mermaid-records', 'mermaid', or 'python -m mermaid_records.cli'."
        ),
    )
    parser.add_argument(
        "--decoder-python",
        type=Path,
        default=None,
        help=(
            "Valid decoder Python used to exercise successful BIN combinations. "
            "If omitted, MERMAID_RECORDS_DECODER_PYTHON is used when present."
        ),
    )
    parser.add_argument(
        "--decoder-script",
        type=Path,
        default=None,
        help=(
            "Valid decoder preprocess.py path used to exercise successful BIN combinations. "
            "If omitted, MERMAID_RECORDS_DECODER_SCRIPT is used when present."
        ),
    )
    parser.add_argument(
        "--mermaid-root",
        type=Path,
        default=None,
        help=(
            "MERMAID root to expose to the external decoder when BIN inputs are present. "
            "Defaults to $MERMAID when set."
        ),
    )
    parser.add_argument(
        "--matrix",
        choices=("semantic", "exhaustive"),
        default="semantic",
        help=(
            "semantic runs one representative set of meaningful flag combinations; "
            "exhaustive expands every boolean permutation."
        ),
    )
    parser.add_argument(
        "--include-invalid",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include intentionally invalid output/decoder resolution combinations and log their errors.",
    )
    parser.add_argument(
        "--include-input-file-mode",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Attempt stateless --input-file combinations in addition to the exact -i/--input-root flow.",
    )
    parser.add_argument(
        "--arg-max-buffer",
        type=int,
        default=32768,
        help="Safety margin subtracted from the detected argv limit before enabling --input-file runs.",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Optional cap for debugging the matrix generator.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI audit harness."""

    args = build_argument_parser().parse_args(argv)
    decoder_python = args.decoder_python or _path_from_env("MERMAID_RECORDS_DECODER_PYTHON")
    decoder_script = args.decoder_script or _path_from_env("MERMAID_RECORDS_DECODER_SCRIPT")
    mermaid_root = args.mermaid_root or _path_from_env("MERMAID")
    report = run_audit(
        input_root=args.input_root.expanduser().resolve(),
        output_root=args.output_root.expanduser().resolve(),
        cli_command=args.cli_command,
        decoder_python=decoder_python,
        decoder_script=decoder_script,
        mermaid_root=mermaid_root,
        include_invalid=args.include_invalid,
        include_input_file_mode=args.include_input_file_mode,
        matrix=args.matrix,
        arg_max_buffer=args.arg_max_buffer,
        max_runs=args.max_runs,
    )
    print(_format_console_summary(report))
    return 0 if report["unexpected_failure_count"] == 0 else 1


def run_audit(
    *,
    input_root: Path,
    output_root: Path,
    cli_command: str,
    decoder_python: Path | None,
    decoder_script: Path | None,
    mermaid_root: Path | None,
    include_invalid: bool,
    include_input_file_mode: bool,
    matrix: str,
    arg_max_buffer: int,
    max_runs: int | None,
) -> dict[str, object]:
    """Execute the normalize matrix and persist reports."""

    discovered = discover_inputs(input_root)
    if not discovered.raw_files:
        raise SystemExit(f"No raw BIN/LOG/MER files found under {input_root}")

    audit_root = output_root / "audit_normalize_cli"
    inputs_dir = audit_root / "inputs"
    runs_dir = audit_root / "runs"
    reports_dir = audit_root / "reports"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    scenarios, skipped_inputs = build_input_scenarios(
        discovered,
        inputs_dir=inputs_dir,
        include_input_file_mode=include_input_file_mode,
        arg_max_buffer=arg_max_buffer,
    )
    specs = build_run_specs(
        scenarios,
        runs_dir=runs_dir,
        cli_command=cli_command,
        decoder_python=decoder_python,
        decoder_script=decoder_script,
        mermaid_root=mermaid_root,
        include_invalid=include_invalid,
        matrix=matrix,
    )
    if max_runs is not None:
        specs = specs[:max_runs]

    results_path = reports_dir / "results.jsonl"
    if results_path.exists():
        results_path.unlink()

    results: list[dict[str, object]] = []
    for skipped in skipped_inputs:
        results.append(skipped)
        _append_jsonl(results_path, skipped)

    for spec in specs:
        record = execute_run_spec(spec)
        payload = _result_payload(spec, record)
        results.append(payload)
        _append_jsonl(results_path, payload)

    summary = summarize_results(
        input_root=input_root,
        output_root=output_root,
        audit_root=audit_root,
        cli_command=cli_command,
        decoder_python=decoder_python,
        decoder_script=decoder_script,
        mermaid_root=mermaid_root,
        discovered=discovered,
        results=results,
    )
    summary_json_path = reports_dir / "summary.json"
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    summary_md_path = reports_dir / "summary.md"
    summary_md_path.write_text(render_markdown_summary(summary), encoding="utf-8")
    return summary


def discover_inputs(input_root: Path) -> DiscoveredInputs:
    """Find raw inputs relevant to the normalize command."""

    raw_files = tuple(
        sorted(
            path.resolve()
            for path in input_root.rglob("*")
            if path.is_file() and path.suffix.upper() in RAW_SUFFIXES
        )
    )
    bin_files = tuple(path for path in raw_files if path.suffix.upper() == ".BIN")
    log_files = tuple(path for path in raw_files if path.suffix.upper() == ".LOG")
    mer_files = tuple(path for path in raw_files if path.suffix.upper() == ".MER")
    return DiscoveredInputs(
        input_root=input_root,
        raw_files=raw_files,
        bin_files=bin_files,
        log_files=log_files,
        mer_files=mer_files,
    )


def build_input_scenarios(
    discovered: DiscoveredInputs,
    *,
    inputs_dir: Path,
    include_input_file_mode: bool,
    arg_max_buffer: int,
) -> tuple[list[InputScenario], list[dict[str, object]]]:
    """Create stateful/stateless input selections and any skip records."""

    inputs_dir.mkdir(parents=True, exist_ok=True)
    raw_manifest = inputs_dir / "all_raw_files.txt"
    raw_manifest.write_text(
        "".join(f"{path.as_posix()}\n" for path in discovered.raw_files),
        encoding="utf-8",
    )

    scenarios = [
        InputScenario(
            name="stateful_input_root",
            input_mode=INPUT_ROOT_MODE,
            input_root=discovered.input_root,
            input_files=(),
            has_bin=discovered.has_bin,
            manifest_path=None,
        )
    ]
    skipped: list[dict[str, object]] = []
    if not include_input_file_mode:
        skipped.append(
            _skipped_payload(
                run_id="skip-input-file-mode",
                slug="skip-input-file-mode",
                summary="Stateless --input-file combinations were disabled for this audit run.",
                details={
                    "input_mode": INPUT_FILE_MODE,
                    "manifest_path": raw_manifest.as_posix(),
                },
            )
        )
        return scenarios, skipped

    argv_limit = _effective_arg_limit(arg_max_buffer)
    stateless_argv_cost = estimate_input_file_argv_cost(discovered.raw_files)
    if stateless_argv_cost > argv_limit:
        skipped.append(
            _skipped_payload(
                run_id="skip-input-file-argv-limit",
                slug="skip-input-file-argv-limit",
                summary=(
                    "Stateless --input-file combinations were skipped because the discovered "
                    "file list would likely exceed the process argv limit."
                ),
                details={
                    "input_mode": INPUT_FILE_MODE,
                    "manifest_path": raw_manifest.as_posix(),
                    "estimated_argv_bytes": stateless_argv_cost,
                    "effective_arg_limit": argv_limit,
                },
            )
        )
        return scenarios, skipped

    scenarios.append(
        InputScenario(
            name="stateless_input_file",
            input_mode=INPUT_FILE_MODE,
            input_root=discovered.input_root,
            input_files=discovered.raw_files,
            has_bin=discovered.has_bin,
            manifest_path=raw_manifest,
        )
    )
    return scenarios, skipped


def build_run_specs(
    scenarios: list[InputScenario],
    *,
    runs_dir: Path,
    cli_command: str,
    decoder_python: Path | None,
    decoder_script: Path | None,
    mermaid_root: Path | None,
    include_invalid: bool,
    matrix: str,
) -> list[RunSpec]:
    """Build the exhaustive run matrix."""

    cli_prefix = tuple(shlex.split(cli_command))
    if not cli_prefix:
        raise SystemExit("--cli-command resolved to an empty command")

    output_modes: tuple[str, ...] = ("cli_arg", "mermaid_env")
    if include_invalid:
        output_modes = output_modes + ("missing",)

    decoder_choices = build_decoder_choices(
        decoder_python=decoder_python,
        decoder_script=decoder_script,
        include_invalid=include_invalid,
    )

    flag_presets = build_flag_presets(matrix)
    specs: list[RunSpec] = []
    counter = 0
    for scenario in scenarios:
        for output_mode in output_modes:
            for decoder_choice in decoder_choices:
                for preflight_mode in ("strict", "cached"):
                    for dry_run, force_rewrite, json_output, verbose in flag_presets:
                        counter += 1
                        slug = _build_slug(
                            scenario=scenario,
                            output_mode=output_mode,
                            decoder_mode=decoder_choice.name,
                            preflight_mode=preflight_mode,
                            dry_run=dry_run,
                            force_rewrite=force_rewrite,
                            json_output=json_output,
                            verbose=verbose,
                        )
                        run_id = f"run-{counter:04d}"
                        artifacts_dir = runs_dir / run_id
                        command, output_dir, env_overrides, seed_decoder_database = compose_command(
                            cli_prefix=cli_prefix,
                            scenario=scenario,
                            output_mode=output_mode,
                            decoder_choice=decoder_choice,
                            preflight_mode=preflight_mode,
                            dry_run=dry_run,
                            force_rewrite=force_rewrite,
                            json_output=json_output,
                            verbose=verbose,
                            artifacts_dir=artifacts_dir,
                            mermaid_root=mermaid_root,
                        )
                        expects_success = expected_success(
                            has_bin=scenario.has_bin,
                            output_mode=output_mode,
                            decoder_choice=decoder_choice,
                            dry_run=dry_run,
                            json_output=json_output,
                            mermaid_root=mermaid_root,
                        )
                        availability_issue = availability_issue_for(
                            has_bin=scenario.has_bin,
                            decoder_choice=decoder_choice,
                            mermaid_root=mermaid_root,
                        )
                        specs.append(
                            RunSpec(
                                run_id=run_id,
                                slug=slug,
                                input_scenario=scenario.name,
                                input_mode=scenario.input_mode,
                                output_mode=output_mode,
                                preflight_mode=preflight_mode,
                                dry_run=dry_run,
                                force_rewrite=force_rewrite,
                                json_output=json_output,
                                verbose=verbose,
                                decoder_mode=decoder_choice.name,
                                expects_success=expects_success,
                                availability_issue=availability_issue,
                                command=command,
                                output_dir=output_dir,
                                artifacts_dir=artifacts_dir,
                                env_overrides=env_overrides,
                                input_manifest_path=scenario.manifest_path,
                                has_bin=scenario.has_bin,
                                decoder_mermaid_root=mermaid_root,
                                seed_decoder_database=seed_decoder_database,
                            )
                        )
    return specs


def build_flag_presets(matrix: str) -> list[tuple[bool, bool, bool, bool]]:
    """Return either the full boolean matrix or a semantic subset."""

    if matrix == "exhaustive":
        return [
            (dry_run, force_rewrite, json_output, verbose)
            for dry_run in (False, True)
            for force_rewrite in (False, True)
            for json_output in (False, True)
            for verbose in (False, True)
        ]
    return [
        (False, False, False, False),
        (False, False, False, True),
        (False, True, False, False),
        (True, False, False, False),
        (True, False, True, False),
        (True, True, True, True),
    ]


def build_decoder_choices(
    *,
    decoder_python: Path | None,
    decoder_script: Path | None,
    include_invalid: bool,
) -> list[DecoderConfigChoice]:
    """Return decoder resolution variants for the matrix."""

    valid_pair_available = decoder_python is not None and decoder_script is not None
    choices = [
        DecoderConfigChoice(
            name="none",
            cli_python=None,
            cli_script=None,
            env_python=None,
            env_script=None,
            available=True,
            note=None,
        ),
        DecoderConfigChoice(
            name="env_both",
            cli_python=None,
            cli_script=None,
            env_python=decoder_python,
            env_script=decoder_script,
            available=valid_pair_available,
            note="Requires valid --decoder-python and --decoder-script inputs to the audit harness.",
        ),
        DecoderConfigChoice(
            name="cli_both",
            cli_python=decoder_python,
            cli_script=decoder_script,
            env_python=None,
            env_script=None,
            available=valid_pair_available,
            note="Requires valid --decoder-python and --decoder-script inputs to the audit harness.",
        ),
        DecoderConfigChoice(
            name="cli_over_env",
            cli_python=decoder_python,
            cli_script=decoder_script,
            env_python=decoder_python,
            env_script=decoder_script,
            available=valid_pair_available,
            note="Requires valid --decoder-python and --decoder-script inputs to the audit harness.",
        ),
    ]
    if include_invalid:
        choices.extend(
            [
                DecoderConfigChoice(
                    name="env_python_only",
                    cli_python=None,
                    cli_script=None,
                    env_python=decoder_python,
                    env_script=None,
                    available=decoder_python is not None,
                    note="Requires a valid decoder Python input to exercise this partial env error case.",
                ),
                DecoderConfigChoice(
                    name="env_script_only",
                    cli_python=None,
                    cli_script=None,
                    env_python=None,
                    env_script=decoder_script,
                    available=decoder_script is not None,
                    note="Requires a valid decoder script input to exercise this partial env error case.",
                ),
                DecoderConfigChoice(
                    name="cli_python_only",
                    cli_python=decoder_python,
                    cli_script=None,
                    env_python=None,
                    env_script=None,
                    available=decoder_python is not None,
                    note="Requires a valid decoder Python input to exercise this partial CLI error case.",
                ),
                DecoderConfigChoice(
                    name="cli_script_only",
                    cli_python=None,
                    cli_script=decoder_script,
                    env_python=None,
                    env_script=None,
                    available=decoder_script is not None,
                    note="Requires a valid decoder script input to exercise this partial CLI error case.",
                ),
            ]
        )
    return choices


def compose_command(
    *,
    cli_prefix: tuple[str, ...],
    scenario: InputScenario,
    output_mode: str,
    decoder_choice: DecoderConfigChoice,
    preflight_mode: str,
    dry_run: bool,
    force_rewrite: bool,
    json_output: bool,
    verbose: bool,
    artifacts_dir: Path,
    mermaid_root: Path | None,
) -> tuple[tuple[str, ...], Path | None, dict[str, str | None], bool]:
    """Assemble one subprocess invocation and its environment overrides."""

    output_dir: Path | None = None
    env_overrides: dict[str, str | None] = {
        "MERMAID": None,
        "MERMAID_RECORDS_DECODER_PYTHON": None,
        "MERMAID_RECORDS_DECODER_SCRIPT": None,
    }
    seed_decoder_database = False
    command = list(cli_prefix)
    command.append("normalize")

    if scenario.input_mode == INPUT_ROOT_MODE:
        command.extend(["-i", scenario.input_root.as_posix()])
    else:
        command.append("--input-file")
        command.extend(path.as_posix() for path in scenario.input_files)

    if output_mode == "cli_arg":
        output_dir = artifacts_dir / "records"
        command.extend(["-o", output_dir.as_posix()])
        if scenario.has_bin and mermaid_root is not None:
            env_overrides["MERMAID"] = mermaid_root.as_posix()
    elif output_mode == "mermaid_env":
        mermaid_root = artifacts_dir / "mermaid_env"
        env_overrides["MERMAID"] = mermaid_root.as_posix()
        output_dir = mermaid_root / "records"
        if scenario.has_bin:
            seed_decoder_database = True

    if decoder_choice.cli_python is not None:
        command.extend(["--decoder-python", decoder_choice.cli_python.as_posix()])
    if decoder_choice.cli_script is not None:
        command.extend(["--decoder-script", decoder_choice.cli_script.as_posix()])
    if decoder_choice.env_python is not None:
        env_overrides["MERMAID_RECORDS_DECODER_PYTHON"] = decoder_choice.env_python.as_posix()
    if decoder_choice.env_script is not None:
        env_overrides["MERMAID_RECORDS_DECODER_SCRIPT"] = decoder_choice.env_script.as_posix()

    command.extend(["--preflight-mode", preflight_mode])
    if dry_run:
        command.append("--dry-run")
    if force_rewrite:
        command.append("--force-rewrite")
    if json_output:
        command.append("--json")
    if verbose:
        command.append("--verbose")
    return tuple(command), output_dir, env_overrides, seed_decoder_database


def expected_success(
    *,
    has_bin: bool,
    output_mode: str,
    decoder_choice: DecoderConfigChoice,
    dry_run: bool,
    json_output: bool,
    mermaid_root: Path | None,
) -> bool:
    """Predict whether the CLI should succeed for one spec."""

    if output_mode == "missing":
        return False
    if json_output and not dry_run:
        return False
    if not decoder_choice.available:
        return False
    if decoder_choice.name in {
        "env_python_only",
        "env_script_only",
        "cli_python_only",
        "cli_script_only",
    }:
        return False
    if has_bin and mermaid_root is None:
        return False
    if has_bin and decoder_choice.name == "none":
        return False
    return True


def availability_issue_for(
    *,
    has_bin: bool,
    decoder_choice: DecoderConfigChoice,
    mermaid_root: Path | None,
) -> str | None:
    """Explain why a planned run cannot meaningfully exercise its intended path."""

    if decoder_choice.available:
        if has_bin and mermaid_root is None:
            return "BIN combinations need a MERMAID root so the external decoder can locate its database."
        return None
    if has_bin and decoder_choice.name in {"env_both", "cli_both", "cli_over_env"}:
        return (
            "This BIN combination needs a valid decoder Python and decoder script supplied to "
            "the audit harness."
        )
    return decoder_choice.note


def execute_run_spec(spec: RunSpec) -> RunResult:
    """Execute one spec and capture structured artifacts."""

    started_epoch = time.time()
    started_perf = time.perf_counter()
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(started_epoch))
    spec.artifacts_dir.mkdir(parents=True, exist_ok=True)

    if spec.availability_issue is not None:
        completed_at = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
        return RunResult(
            run_id=spec.run_id,
            slug=spec.slug,
            status="skipped",
            returncode=None,
            duration_s=0.0,
            started_at=started_at,
            completed_at=completed_at,
            stdout_path=None,
            stderr_path=None,
            stdout_preview="",
            stderr_preview="",
            exception=None,
            summary=spec.availability_issue,
        )

    if spec.seed_decoder_database:
        _seed_decoder_database(spec)

    env = os.environ.copy()
    for key in ("MERMAID", "MERMAID_RECORDS_DECODER_PYTHON", "MERMAID_RECORDS_DECODER_SCRIPT"):
        env.pop(key, None)
    for key, value in spec.env_overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value

    stdout_path = spec.artifacts_dir / "stdout.txt"
    stderr_path = spec.artifacts_dir / "stderr.txt"
    command_path = spec.artifacts_dir / "command.txt"
    command_path.write_text(" ".join(shlex.quote(token) for token in spec.command), encoding="utf-8")
    metadata_path = spec.artifacts_dir / "spec.json"
    metadata_path.write_text(
        json.dumps(
            {
                "run_id": spec.run_id,
                "slug": spec.slug,
                "input_scenario": spec.input_scenario,
                "input_mode": spec.input_mode,
                "output_mode": spec.output_mode,
                "decoder_mode": spec.decoder_mode,
                "preflight_mode": spec.preflight_mode,
                "dry_run": spec.dry_run,
                "force_rewrite": spec.force_rewrite,
                "json_output": spec.json_output,
                "verbose": spec.verbose,
                "expects_success": spec.expects_success,
                "output_dir": spec.output_dir.as_posix() if spec.output_dir else None,
                "command": list(spec.command),
                "env_overrides": spec.env_overrides,
                "input_manifest_path": (
                    spec.input_manifest_path.as_posix() if spec.input_manifest_path else None
                ),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    try:
        completed = subprocess.run(
            spec.command,
            capture_output=True,
            text=True,
            env=env,
            cwd=Path.cwd(),
            check=False,
        )
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        stderr_path.write_text(completed.stderr, encoding="utf-8")
        duration_s = time.perf_counter() - started_perf
        status = classify_result(spec, completed.returncode)
        completed_at = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
        return RunResult(
            run_id=spec.run_id,
            slug=spec.slug,
            status=status,
            returncode=completed.returncode,
            duration_s=duration_s,
            started_at=started_at,
            completed_at=completed_at,
            stdout_path=stdout_path.as_posix(),
            stderr_path=stderr_path.as_posix(),
            stdout_preview=preview_text(completed.stdout),
            stderr_preview=preview_text(completed.stderr),
            exception=None,
            summary=summarize_completed_run(spec, completed.returncode, completed.stdout, completed.stderr),
        )
    except Exception as exc:  # pragma: no cover - exercised via tests with fake executables
        duration_s = time.perf_counter() - started_perf
        completed_at = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
        stderr_path.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
        return RunResult(
            run_id=spec.run_id,
            slug=spec.slug,
            status="unexpected_failure" if spec.expects_success else "expected_error",
            returncode=None,
            duration_s=duration_s,
            started_at=started_at,
            completed_at=completed_at,
            stdout_path=None,
            stderr_path=stderr_path.as_posix(),
            stdout_preview="",
            stderr_preview=f"{type(exc).__name__}: {exc}",
            exception=f"{type(exc).__name__}: {exc}",
            summary=f"Subprocess launch failed: {type(exc).__name__}: {exc}",
        )


def classify_result(spec: RunSpec, returncode: int) -> str:
    """Classify a subprocess return code against expectations."""

    if returncode == 0 and spec.expects_success:
        return "success"
    if returncode != 0 and not spec.expects_success:
        return "expected_error"
    if returncode == 0 and not spec.expects_success:
        return "unexpected_success"
    return "unexpected_failure"


def summarize_completed_run(spec: RunSpec, returncode: int, stdout: str, stderr: str) -> str:
    """Create a short human summary for one completed process."""

    if returncode == 0:
        if spec.dry_run and spec.json_output:
            return "Completed successfully with dry-run JSON output."
        return "Completed successfully."

    error_text = first_nonempty_line(stderr) or first_nonempty_line(stdout) or "non-zero exit"
    return f"Exited with code {returncode}: {error_text}"


def summarize_results(
    *,
    input_root: Path,
    output_root: Path,
    audit_root: Path,
    cli_command: str,
    decoder_python: Path | None,
    decoder_script: Path | None,
    mermaid_root: Path | None,
    discovered: DiscoveredInputs,
    results: list[dict[str, object]],
) -> dict[str, object]:
    """Aggregate result records into JSON/Markdown-friendly summaries."""

    status_counts = Counter(str(result["status"]) for result in results)
    unexpected = [
        result
        for result in results
        if result["status"] in {"unexpected_failure", "unexpected_success"}
    ]
    skipped = [result for result in results if result["status"] == "skipped"]
    json_without_dry_run_errors = [
        result
        for result in results
        if result.get("status") == "expected_error"
        and result.get("json_output") is True
        and result.get("dry_run") is False
    ]
    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        "input_root": input_root.as_posix(),
        "output_root": output_root.as_posix(),
        "audit_root": audit_root.as_posix(),
        "cli_command": cli_command,
        "decoder_python": decoder_python.as_posix() if decoder_python else None,
        "decoder_script": decoder_script.as_posix() if decoder_script else None,
        "mermaid_root": mermaid_root.as_posix() if mermaid_root else None,
        "discovered_counts": discovered.counts(),
        "total_result_rows": len(results),
        "status_counts": dict(status_counts),
        "unexpected_failure_count": status_counts["unexpected_failure"] + status_counts["unexpected_success"],
        "skipped_count": status_counts["skipped"],
        "json_without_dry_run_error_count": len(json_without_dry_run_errors),
        "results_path": (audit_root / "reports" / "results.jsonl").as_posix(),
        "summary_json_path": (audit_root / "reports" / "summary.json").as_posix(),
        "summary_md_path": (audit_root / "reports" / "summary.md").as_posix(),
        "unexpected_results": unexpected[:25],
        "skipped_examples": skipped[:25],
    }
    return summary


def render_markdown_summary(summary: dict[str, object]) -> str:
    """Render a concise Markdown report for humans."""

    discovered_counts = summary["discovered_counts"]
    status_counts = summary["status_counts"]
    unexpected_results = summary["unexpected_results"]
    skipped_examples = summary["skipped_examples"]
    lines = [
        "# Normalize CLI Audit",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- CLI command: `{summary['cli_command']}`",
        f"- Input root: `{summary['input_root']}`",
        f"- Output root: `{summary['output_root']}`",
        f"- Results JSONL: `{summary['results_path']}`",
        "",
        "## Discovered Inputs",
        "",
        f"- Raw files: `{discovered_counts['raw_files']}`",
        f"- BIN files: `{discovered_counts['bin_files']}`",
        f"- LOG files: `{discovered_counts['log_files']}`",
        f"- MER files: `{discovered_counts['mer_files']}`",
        "",
        "## Outcome Counts",
        "",
    ]
    for key in sorted(status_counts):
        lines.append(f"- {key}: `{status_counts[key]}`")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            f"- `--json` without `--dry-run` expected-error cases: `{summary['json_without_dry_run_error_count']}`",
            f"- Skipped combinations: `{summary['skipped_count']}`",
            "",
        ]
    )
    if unexpected_results:
        lines.extend(["## Unexpected Results", ""])
        for row in unexpected_results:
            lines.append(
                f"- `{row['run_id']}` `{row['slug']}`: {row['summary']} "
                f"(stderr: `{row.get('stderr_path')}`)"
            )
        lines.append("")
    if skipped_examples:
        lines.extend(["## Skipped Examples", ""])
        for row in skipped_examples:
            lines.append(f"- `{row['run_id']}` `{row['slug']}`: {row['summary']}")
        lines.append("")
    return "\n".join(lines)


def _format_console_summary(summary: dict[str, object]) -> str:
    """Print a short terminal summary after the sweep."""

    status_counts = summary["status_counts"]
    parts = [
        "Normalize CLI audit complete",
        f"  input root: {summary['input_root']}",
        f"  result rows: {summary['total_result_rows']}",
    ]
    for status in sorted(status_counts):
        parts.append(f"  {status}: {status_counts[status]}")
    parts.extend(
        [
            f"  unexpected: {summary['unexpected_failure_count']}",
            f"  results jsonl: {summary['results_path']}",
            f"  summary md: {summary['summary_md_path']}",
        ]
    )
    return "\n".join(parts)


def preview_text(text: str, *, max_lines: int = 8, max_chars: int = 600) -> str:
    """Return a short preview safe for JSONL rows."""

    clipped = "\n".join(text.splitlines()[:max_lines]).strip()
    if len(clipped) > max_chars:
        return clipped[: max_chars - 3] + "..."
    return clipped


def first_nonempty_line(text: str) -> str | None:
    """Return the first non-empty line, if any."""

    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def estimate_input_file_argv_cost(paths: tuple[Path, ...]) -> int:
    """Estimate argv bytes for the explicit file list."""

    return sum(len(path.as_posix()) + 1 for path in paths) + len("--input-file") + 1


def _effective_arg_limit(arg_max_buffer: int) -> int:
    """Compute a conservative argv budget."""

    detected = None
    if hasattr(os, "sysconf"):
        try:
            detected = int(os.sysconf("SC_ARG_MAX"))
        except (OSError, ValueError):
            detected = None
    if detected is None or detected <= arg_max_buffer:
        return 131072
    return detected - arg_max_buffer


def _build_slug(
    *,
    scenario: InputScenario,
    output_mode: str,
    decoder_mode: str,
    preflight_mode: str,
    dry_run: bool,
    force_rewrite: bool,
    json_output: bool,
    verbose: bool,
) -> str:
    """Build a stable readable slug for one run."""

    return (
        f"{scenario.input_mode}-{output_mode}-{decoder_mode}-{preflight_mode}"
        f"-dry{int(dry_run)}-force{int(force_rewrite)}-json{int(json_output)}-verbose{int(verbose)}"
    )


def _result_payload(spec: RunSpec, result: RunResult) -> dict[str, object]:
    """Combine spec metadata and result metadata for JSONL output."""

    return {
        "run_id": result.run_id,
        "slug": result.slug,
        "status": result.status,
        "summary": result.summary,
        "returncode": result.returncode,
        "duration_s": round(result.duration_s, 6),
        "started_at": result.started_at,
        "completed_at": result.completed_at,
        "input_scenario": spec.input_scenario,
        "input_mode": spec.input_mode,
        "output_mode": spec.output_mode,
        "preflight_mode": spec.preflight_mode,
        "dry_run": spec.dry_run,
        "force_rewrite": spec.force_rewrite,
        "json_output": spec.json_output,
        "verbose": spec.verbose,
        "decoder_mode": spec.decoder_mode,
        "expects_success": spec.expects_success,
        "availability_issue": spec.availability_issue,
        "has_bin": spec.has_bin,
        "decoder_mermaid_root": (
            spec.decoder_mermaid_root.as_posix() if spec.decoder_mermaid_root else None
        ),
        "seed_decoder_database": spec.seed_decoder_database,
        "command": list(spec.command),
        "output_dir": spec.output_dir.as_posix() if spec.output_dir else None,
        "artifacts_dir": spec.artifacts_dir.as_posix(),
        "stdout_path": result.stdout_path,
        "stderr_path": result.stderr_path,
        "stdout_preview": result.stdout_preview,
        "stderr_preview": result.stderr_preview,
        "exception": result.exception,
        "input_manifest_path": (
            spec.input_manifest_path.as_posix() if spec.input_manifest_path else None
        ),
        "env_overrides": spec.env_overrides,
    }


def _skipped_payload(
    *,
    run_id: str,
    slug: str,
    summary: str,
    details: dict[str, object],
) -> dict[str, object]:
    """Create a JSONL row for non-run skips discovered during planning."""

    return {
        "run_id": run_id,
        "slug": slug,
        "status": "skipped",
        "summary": summary,
        "returncode": None,
        "duration_s": 0.0,
        "started_at": None,
        "completed_at": None,
        "stdout_path": None,
        "stderr_path": None,
        "stdout_preview": "",
        "stderr_preview": "",
        "exception": None,
        **details,
    }


def _append_jsonl(path: Path, payload: dict[str, object]) -> None:
    """Append one structured row to a JSONL file."""

    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _path_from_env(name: str) -> Path | None:
    """Resolve an optional path from the environment."""

    value = os.environ.get(name)
    return Path(value).expanduser().resolve() if value else None


def _seed_decoder_database(spec: RunSpec) -> None:
    """Mirror the configured decoder database into an isolated MERMAID root."""

    if spec.decoder_mermaid_root is None:
        raise RuntimeError("decoder_mermaid_root is required when seed_decoder_database is enabled")

    source_database = spec.decoder_mermaid_root / "database"
    if not source_database.exists():
        raise RuntimeError(f"Decoder MERMAID database directory does not exist: {source_database}")

    target_root = Path(spec.env_overrides["MERMAID"])
    target_database = target_root / "database"
    target_root.mkdir(parents=True, exist_ok=True)
    if target_database.exists():
        return
    target_database.symlink_to(source_database, target_is_directory=True)
