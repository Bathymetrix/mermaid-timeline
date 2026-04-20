# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest
import mermaid_records._audit_normalize_cli as audit_normalize_cli
from mermaid_records._audit_normalize_cli import (
    INPUT_FILE_MODE,
    INPUT_ROOT_MODE,
    build_flag_presets,
    build_input_scenarios,
    build_run_specs,
    discover_inputs,
    expected_success,
    run_audit,
)


def test_build_input_scenarios_skips_stateless_when_argv_budget_is_too_small(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    for index in range(3):
        (input_root / f"sample_{index}.LOG").write_text("1700000000:[MAIN  ,0007]ok\n", encoding="utf-8")

    discovered = discover_inputs(input_root)
    monkeypatch.setattr(audit_normalize_cli, "_effective_arg_limit", lambda _buffer: 10)
    scenarios, skipped = build_input_scenarios(
        discovered,
        inputs_dir=tmp_path / "artifacts",
        include_input_file_mode=True,
        arg_max_buffer=0,
    )

    assert [scenario.input_mode for scenario in scenarios] == [INPUT_ROOT_MODE]
    assert skipped[0]["status"] == "skipped"
    assert skipped[0]["input_mode"] == INPUT_FILE_MODE


def test_build_run_specs_marks_missing_decoder_pairs_as_skipped_for_bin_inputs(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")

    discovered = discover_inputs(input_root)
    scenarios, skipped = build_input_scenarios(
        discovered,
        inputs_dir=tmp_path / "artifacts",
        include_input_file_mode=False,
        arg_max_buffer=0,
    )

    assert skipped

    specs = build_run_specs(
        scenarios,
        runs_dir=tmp_path / "runs",
        cli_command=f"{sys.executable} -m mermaid_records.cli",
        decoder_python=None,
        decoder_script=None,
        mermaid_root=None,
        include_invalid=False,
        matrix="semantic",
    )

    env_both_specs = [spec for spec in specs if spec.decoder_mode == "env_both"]
    assert env_both_specs
    assert all(spec.availability_issue is not None for spec in env_both_specs)
    assert all(spec.expects_success is False for spec in env_both_specs)


def test_semantic_flag_presets_are_reduced_and_exhaustive_has_all_boolean_combinations() -> None:
    semantic = build_flag_presets("semantic")
    exhaustive = build_flag_presets("exhaustive")

    assert semantic == [
        (False, False, False, False),
        (False, False, False, True),
        (False, True, False, False),
        (True, False, False, False),
        (True, False, True, False),
        (True, True, True, True),
    ]
    assert len(exhaustive) == 16


def test_build_run_specs_propagates_mermaid_root_for_bin_cli_arg_runs(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")
    decoder_python = tmp_path / "decoder-python"
    decoder_python.write_text("", encoding="utf-8")
    decoder_script = tmp_path / "decoder-script.py"
    decoder_script.write_text("", encoding="utf-8")
    mermaid_root = tmp_path / "mermaid"
    (mermaid_root / "database").mkdir(parents=True)

    discovered = discover_inputs(input_root)
    scenarios, _ = build_input_scenarios(
        discovered,
        inputs_dir=tmp_path / "artifacts",
        include_input_file_mode=False,
        arg_max_buffer=0,
    )

    specs = build_run_specs(
        scenarios,
        runs_dir=tmp_path / "runs",
        cli_command=f"{sys.executable} -m mermaid_records.cli",
        decoder_python=decoder_python,
        decoder_script=decoder_script,
        mermaid_root=mermaid_root,
        include_invalid=False,
        matrix="semantic",
    )

    cli_both = next(spec for spec in specs if spec.decoder_mode == "cli_both" and spec.output_mode == "cli_arg")
    env_both = next(spec for spec in specs if spec.decoder_mode == "env_both" and spec.output_mode == "mermaid_env")

    assert cli_both.env_overrides["MERMAID"] == mermaid_root.as_posix()
    assert cli_both.expects_success is True
    assert env_both.seed_decoder_database is True


def test_expected_success_rejects_json_without_dry_run() -> None:
    assert (
        expected_success(
            has_bin=False,
            output_mode="cli_arg",
            decoder_choice=audit_normalize_cli.DecoderConfigChoice(
                name="none",
                cli_python=None,
                cli_script=None,
                env_python=None,
                env_script=None,
                available=True,
                note=None,
            ),
            dry_run=False,
            json_output=True,
            mermaid_root=None,
        )
        is False
    )


def test_run_audit_logs_success_and_failure_without_stopping(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "0100_sample.LOG").write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )
    fake_cli = tmp_path / "fake_cli.py"
    fake_cli.write_text(
        """
import sys

argv = sys.argv[1:]
if "normalize" not in argv:
    print("missing normalize", file=sys.stderr)
    raise SystemExit(2)
if "--verbose" in argv:
    print("verbose failure", file=sys.stderr)
    raise SystemExit(9)
print("ok")
""".strip()
        + "\n",
        encoding="utf-8",
    )

    summary = run_audit(
        input_root=input_root,
        output_root=tmp_path / "output",
        cli_command=f"{sys.executable} {fake_cli}",
        decoder_python=None,
        decoder_script=None,
        mermaid_root=None,
        include_invalid=False,
        include_input_file_mode=False,
        matrix="semantic",
        arg_max_buffer=0,
        max_runs=2,
    )

    results_path = Path(summary["results_path"])
    rows = [json.loads(line) for line in results_path.read_text(encoding="utf-8").splitlines()]

    assert len(rows) == 3
    assert rows[0]["status"] == "skipped"
    assert rows[1]["status"] == "success"
    assert rows[2]["status"] == "unexpected_failure"
    assert summary["unexpected_failure_count"] == 1
