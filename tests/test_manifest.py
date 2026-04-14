# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from mermaid_records.bin2log import Bin2LogConfig
from mermaid_records.normalize_pipeline import run_normalization_pipeline


def test_stateful_run_writes_per_float_outputs_and_manifests(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "452.020-P-06.vit").write_text("", encoding="utf-8")
    _write_log(input_root / "06_first.LOG", "first")
    _write_mer(input_root / "06_first.MER")

    output_root = tmp_path / "output"
    summary = run_normalization_pipeline(input_root, output_dir=output_root)

    float_dir = output_root / "452.020-P-06"
    latest = _read_json(float_dir / "manifests" / "latest.json")
    run_json = _read_json(float_dir / latest["run_manifest"])
    source_state = _read_json(float_dir / latest["source_state_manifest"])

    assert summary.mode == "stateful"
    assert [item.float_id for item in summary.processed_floats] == ["06"]
    assert (float_dir / "log_operational_records.jsonl").exists()
    assert (float_dir / "mer_environment_records.jsonl").exists()
    assert run_json["status"] == "success"
    assert source_state["input_root"] == input_root.as_posix()
    assert source_state["normalization_version"] == "0.1.0"
    assert {item["source_kind"] for item in source_state["raw_sources"]} == {"log", "mer"}


def test_stateful_append_path_appends_only_new_files(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    _write_log(input_root / "0100_first.LOG", "first")
    output_root = tmp_path / "output"

    run_normalization_pipeline(input_root, output_dir=output_root)
    _write_log(input_root / "0100_second.LOG", "second")
    summary = run_normalization_pipeline(input_root, output_dir=output_root)

    float_summary = summary.processed_floats[0]
    operational_lines = _jsonl_lines(output_root / "467.174-T-0100" / "log_operational_records.jsonl")

    assert float_summary.log_action == "append"
    assert len(operational_lines) == 2
    assert operational_lines[0]["message"] == "first"
    assert operational_lines[1]["message"] == "second"


def test_stateful_rewrite_and_prune_on_changed_or_removed_source(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    log_path = input_root / "0100_first.LOG"
    _write_log(log_path, "first")
    output_root = tmp_path / "output"

    run_normalization_pipeline(input_root, output_dir=output_root)
    _write_log(log_path, "first changed")
    summary = run_normalization_pipeline(input_root, output_dir=output_root)
    lines_after_change = _jsonl_lines(output_root / "467.174-T-0100" / "log_operational_records.jsonl")

    assert summary.processed_floats[0].log_action == "rewrite"
    assert len(lines_after_change) == 1
    assert lines_after_change[0]["message"] == "first changed"

    log_path.unlink()
    summary = run_normalization_pipeline(input_root, output_dir=output_root)
    pruned_lines = _jsonl_lines(output_root / "467.174-T-0100" / "state" / "pruned_records.jsonl")

    assert summary.processed_floats[0].log_action == "rewrite"
    assert not (output_root / "467.174-T-0100" / "log_operational_records.jsonl").exists()
    assert pruned_lines[-1]["source_file"] == log_path.as_posix()
    assert pruned_lines[-1]["source_kind"] == "log"


def test_decoder_state_invalidates_only_bin_dependent_float(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "0100_first.BIN").write_bytes(b"raw-bin")
    _write_log(input_root / "0200_first.LOG", "plain log")

    mermaid_root = tmp_path / "mermaid_root"
    database_root = mermaid_root / "database"
    database_root.mkdir(parents=True)
    (database_root / "Databases.json").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("MERMAID", str(mermaid_root))

    decoder_a = _write_decoder(tmp_path / "decoder_a.py", "decoded a")
    decoder_b = _write_decoder(tmp_path / "decoder_b.py", "decoded b")
    output_root = tmp_path / "output"

    run_normalization_pipeline(
        input_root,
        output_dir=output_root,
        config=Bin2LogConfig(
            python_executable=Path(sys.executable),
            decoder_script=decoder_a,
        ),
    )
    log_only_before = (output_root / "0200" / "log_operational_records.jsonl").read_text(encoding="utf-8")

    summary = run_normalization_pipeline(
        input_root,
        output_dir=output_root,
        config=Bin2LogConfig(
            python_executable=Path(sys.executable),
            decoder_script=decoder_b,
        ),
    )

    by_float = {item.float_id: item for item in summary.processed_floats}
    bin_lines = _jsonl_lines(output_root / "0100" / "log_operational_records.jsonl")
    log_only_after = (output_root / "0200" / "log_operational_records.jsonl").read_text(encoding="utf-8")

    assert by_float["0100"].decoder_state_invalidated is True
    assert by_float["0100"].log_action == "rewrite"
    assert by_float["0200"].decoder_state_invalidated is False
    assert by_float["0200"].log_action == "noop"
    assert bin_lines[0]["message"] == "decoded b"
    assert log_only_before == log_only_after


def test_stateless_mode_isolated_and_rejects_existing_manifests(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    log_path = input_root / "0100_first.LOG"
    _write_log(log_path, "first")

    output_root = tmp_path / "output"
    summary = run_normalization_pipeline(
        output_dir=output_root,
        input_files=[log_path],
    )

    assert summary.mode == "stateless"
    assert not (output_root / "0100" / "manifests").exists()
    assert (output_root / "0100" / "log_operational_records.jsonl").exists()

    manifests_dir = output_root / "0200" / "manifests"
    manifests_dir.mkdir(parents=True)
    (manifests_dir / "latest.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="already contains manifests"):
        run_normalization_pipeline(
            output_dir=output_root,
            input_files=[log_path],
        )


def _write_log(path: Path, message: str) -> None:
    path.write_text(f"1700000000:[MAIN  ,0007]{message}\n", encoding="utf-8")


def _write_mer(path: Path) -> None:
    path.write_bytes(
        (
            "<ENVIRONMENT>\n"
            "\t<BOARD 452116600-A0 />\n"
            "</ENVIRONMENT>\n"
            "<PARAMETERS>\n"
            "\t<MISC UPLOAD_MAX=100kB />\n"
            "</PARAMETERS>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-07T22:47:22 FNAME=2024-02-07T22_47_22.000000 "
            "SMP_OFFSET=614054 TRUE_FS=40.014107 />\n"
            "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            "STAGES=5 NORMALIZED=YES LENGTH=4832 />\n"
            "\t<DATA>ABC</DATA>\n"
            "</EVENT>\n"
        ).encode("ascii")
    )


def _write_decoder(path: Path, message: str) -> Path:
    path.write_text(
        f"""
from pathlib import Path

def database_update(_arg):
    print("Update Databases")

def concatenate_files(path):
    return [path]

def concatenate_rbr_files(path):
    return [path]

def decrypt_all(path):
    workdir = Path(path)
    log = workdir / "0100_first.LOG"
    log.write_text("1700000000:[MAIN  ,0007]{message}\\n", encoding="utf-8")
    return [path]
""",
        encoding="utf-8",
    )
    return path


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl_lines(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
