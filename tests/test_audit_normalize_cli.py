# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest
import mermaid_records.audit_normalize_cli as audit_normalize_cli
from mermaid_records.audit_normalize_cli import (
    INPUT_FILE_MODE,
    INPUT_ROOT_MODE,
    build_input_scenarios,
    build_run_specs,
    discover_inputs,
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
        include_invalid=False,
    )

    env_both_specs = [spec for spec in specs if spec.decoder_mode == "env_both"]
    assert env_both_specs
    assert all(spec.availability_issue is not None for spec in env_both_specs)
    assert all(spec.expects_success is False for spec in env_both_specs)


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
        include_invalid=False,
        include_input_file_mode=False,
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
