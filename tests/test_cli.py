# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path

from mermaid_records.cli import build_parser, main


def test_cli_help_exposes_only_normalize_subcommand() -> None:
    help_text = build_parser().format_help()

    assert "normalize" in help_text
    assert "inspect-mer" not in help_text


def test_normalize_cli_writes_log_and_mer_jsonl_outputs(tmp_path: Path, capsys) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")

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

    output_dir = tmp_path / "output"

    result = main(
        [
            "normalize",
            "-i",
            str(input_root),
            "-o",
            str(output_dir),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert result == 0
    assert payload["input_root"] == input_root.as_posix()
    assert payload["output_dir"] == output_dir.as_posix()
    assert payload["mode"] == "stateful"
    assert payload["processed_floats"][0]["float_id"] == "0100"
    assert (output_dir / "467.174-T-0100" / "log_operational_records.jsonl").exists()
    assert (output_dir / "467.174-T-0100" / "mer_environment_records.jsonl").exists()
    assert not (output_dir / "467.174-T-0100" / "preflight_status.json").exists()


def test_normalize_cli_accepts_comma_and_space_separated_input_files(tmp_path: Path, capsys) -> None:
    log_a = tmp_path / "0100_a.LOG"
    log_b = tmp_path / "0100_b.LOG"
    log_c = tmp_path / "0100_c.LOG"
    log_a.write_text("1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n", encoding="utf-8")
    log_b.write_text("1700000001:[MAIN  ,0007]second\n", encoding="utf-8")
    log_c.write_text("1700000002:[MAIN  ,0007]third\n", encoding="utf-8")

    output_dir = tmp_path / "output"
    result = main(
        [
            "normalize",
            "--input-file",
            f"{log_a},{log_b}",
            str(log_c),
            "-o",
            str(output_dir),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert result == 0
    assert payload["mode"] == "stateless"
    assert payload["input_files"] == [
        log_a.as_posix(),
        log_b.as_posix(),
        log_c.as_posix(),
    ]
    assert (output_dir / "467.174-T-0100" / "log_operational_records.jsonl").exists()


def test_normalize_cli_dry_run_human_output_is_side_effect_free(tmp_path: Path, capsys) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    log_path = input_root / "0100_sample.LOG"
    log_path.write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "output"
    result = main(
        [
            "normalize",
            "-i",
            str(input_root),
            "-o",
            str(output_dir),
            "--dry-run",
        ]
    )

    captured = capsys.readouterr()

    assert result == 0
    assert "FLOAT 467.174-T-0100" in captured.out
    assert "log: append" in captured.out
    assert not output_dir.exists()


def test_normalize_cli_dry_run_json_output(tmp_path: Path, capsys) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    log_path = input_root / "0100_sample.LOG"
    log_path.write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "output"
    result = main(
        [
            "normalize",
            "-i",
            str(input_root),
            "-o",
            str(output_dir),
            "--dry-run",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert result == 0
    assert payload["mode"] == "stateful"
    assert payload["floats"][0]["families"]["log"]["action"] == "append"
    assert payload["floats"][0]["families"]["log"]["file_diffs"][0]["change_kind"] == "new"
    assert not output_dir.exists()
