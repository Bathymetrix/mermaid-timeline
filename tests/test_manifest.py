# SPDX-License-Identifier: MIT

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import platform
import sys

import pytest

from mermaid_records.bin2log import Bin2LogConfig, Bin2LogError
from mermaid_records.normalize_pipeline import run_normalization_pipeline


def test_normalization_pipeline_writes_manifests_for_log_mer_run(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    log_path = input_root / "0100_sample.LOG"
    log_path.write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )
    mer_path = input_root / "0100_sample.MER"
    mer_path.write_bytes(
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
    output_root = tmp_path / "output"

    summary = run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "manifests" / "latest.json")
    run_dir = output_root / Path(latest["run_manifest"]).parent
    run_json = _read_json(output_root / latest["run_manifest"])
    outputs_json = _read_json(output_root / latest["outputs_manifest"])
    source_state = _read_json(output_root / latest["source_state_manifest"])

    assert summary.bin_count == 0
    assert run_json["status"] == "success"
    assert run_json["preflight_mode"] is None
    assert source_state["decoder_state"] is None
    assert {item["source_kind"] for item in source_state["raw_sources"]} == {"log", "mer"}
    assert sorted(item["source_file"] for item in source_state["raw_sources"]) == sorted(
        [log_path.as_posix(), mer_path.as_posix()]
    )
    assert outputs_json["counts"]["log_operational_records"] == 1
    assert outputs_json["counts"]["mer_environment_records"] == 1
    assert any(
        item["path"] == "log_jsonl/log_operational_records.jsonl"
        for item in outputs_json["jsonl_outputs"]
    )
    assert any(
        item["path"] == "mer_jsonl/mer_environment_records.jsonl"
        for item in outputs_json["jsonl_outputs"]
    )
    assert run_dir.exists()


def test_normalization_pipeline_writes_decoder_state_for_bin_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    bin_path = input_root / "0100_sample.BIN"
    bin_path.write_bytes(b"raw-bin")

    mermaid_root = tmp_path / "mermaid_root"
    database_root = mermaid_root / "database"
    database_root.mkdir(parents=True)
    for name in (
        "Databases.json",
        "DatabaseV1_0.json",
        "MarittimoV1_1.json",
        "MultimerV1_0.json",
        "UniversalV1_0.json",
    ):
        (database_root / name).write_text(f'{{"name": "{name}"}}\n', encoding="utf-8")
    monkeypatch.setenv("MERMAID", str(mermaid_root))

    decoder_script = tmp_path / "fake_decoder.py"
    decoder_script.write_text(
        """
from pathlib import Path

def database_update(_arg):
    print("Update Databases")

def concatenate_files(path):
    return [path]

def concatenate_rbr_files(path):
    return [path]

def decrypt_all(path):
    workdir = Path(path)
    log = workdir / "0100_sample.LOG"
    log.write_text("1700000000:[MAIN  ,0007]decoded\\n", encoding="utf-8")
    return [path]
""",
        encoding="utf-8",
    )

    output_root = tmp_path / "output"
    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=decoder_script,
    )

    run_normalization_pipeline(input_root, output_dir=output_root, config=config)

    latest = _read_json(output_root / "manifests" / "latest.json")
    run_json = _read_json(output_root / latest["run_manifest"])
    source_state = _read_json(output_root / latest["source_state_manifest"])
    preflight_run = _read_json(output_root / latest["preflight_status"])
    preflight_root = _read_json(output_root / "preflight_status.json")

    decoder_state = source_state["decoder_state"]
    assert run_json["status"] == "success"
    assert decoder_state["decoder_python"] == str(Path(sys.executable))
    assert decoder_state["decoder_python_version"] == platform.python_version()
    assert decoder_state["decoder_script"] == str(decoder_script)
    assert decoder_state["decoder_script_hash"] == _hash_file(decoder_script)
    assert decoder_state["preflight_mode"] == "strict"
    assert decoder_state["decoder_git_commit"] is None
    assert decoder_state["database_bundle_hash"] is not None
    assert decoder_state["database_files"] == [
        (database_root / name).as_posix()
        for name in sorted(
            [
                "DatabaseV1_0.json",
                "Databases.json",
                "MarittimoV1_1.json",
                "MultimerV1_0.json",
                "UniversalV1_0.json",
            ]
        )
    ]
    assert preflight_run == preflight_root


def test_normalization_pipeline_writes_failed_run_manifests_on_strict_preflight_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")

    mermaid_root = tmp_path / "mermaid_root"
    database_root = mermaid_root / "database"
    database_root.mkdir(parents=True)
    (database_root / "Databases.json").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("MERMAID", str(mermaid_root))

    decoder_script = tmp_path / "fake_decoder_fail.py"
    decoder_script.write_text(
        """
def database_update(_arg):
    print("Update Databases")
    print('Exception: "simulated refresh failure" detected when get Databases.json')

def concatenate_files(path):
    return [path]

def concatenate_rbr_files(path):
    return [path]

def decrypt_all(path):
    return [path]
""",
        encoding="utf-8",
    )

    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=decoder_script,
        preflight_mode="strict",
    )
    output_root = tmp_path / "output"

    with pytest.raises(Bin2LogError):
        run_normalization_pipeline(input_root, output_dir=output_root, config=config)

    latest = _read_json(output_root / "manifests" / "latest.json")
    run_json = _read_json(output_root / latest["run_manifest"])
    source_state = _read_json(output_root / latest["source_state_manifest"])

    assert run_json["status"] == "partial"
    assert run_json["preflight_mode"] == "strict"
    assert source_state["decoder_state"]["preflight_mode"] == "strict"
    assert (output_root / latest["preflight_status"]).exists()


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
