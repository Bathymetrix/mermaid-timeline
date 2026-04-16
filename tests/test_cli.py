# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest
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

    assert result == 0
    assert "NORMALIZATION SUMMARY" in captured.out
    assert "mode: stateful" in captured.out
    assert "raw files processed: 2" in captured.out
    assert "log records written=" in captured.out
    assert "mer records written=" in captured.out
    assert "Starting normalization" in captured.err
    assert "Processing instrument T0100" in captured.err
    assert "Normalizing LOG for instrument T0100" in captured.err
    assert "Normalizing MER for instrument T0100" in captured.err
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

    assert result == 0
    assert "NORMALIZATION SUMMARY" in captured.out
    assert "mode: stateless" in captured.out
    assert "raw files processed: 3" in captured.out
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
    assert "DRY RUN SUMMARY" in captured.out
    assert "mode: stateful" in captured.out
    assert "raw files processed: 1" in captured.out
    assert "not evaluated" in captured.out
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
    assert payload["instruments"][0]["families"]["log"]["action"] == "append"
    assert payload["instruments"][0]["families"]["log"]["file_diffs"][0]["change_kind"] == "new"
    assert not output_dir.exists()


def test_normalize_cli_verbose_summary_expands_output(tmp_path: Path, capsys) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    (input_root / "0100_sample.LOG").write_text(
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
            "--verbose",
        ]
    )

    captured = capsys.readouterr()

    assert result == 0
    assert "    family actions:" in captured.out
    assert "      log: append=1 rewrite=0 noop=0" in captured.out
    assert "      mer: append=0 rewrite=0 noop=1" in captured.out
    assert "      per-instrument actions:" in captured.out
    assert f"output root: {output_dir.as_posix()}" in captured.out
    assert f"input root: {input_root.as_posix()}" in captured.out


def test_normalize_cli_short_verbose_flag_expands_output(tmp_path: Path, capsys) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    (input_root / "0100_sample.LOG").write_text(
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
            "-v",
        ]
    )

    captured = capsys.readouterr()

    assert result == 0
    assert "    family actions:" in captured.out
    assert "      per-instrument actions:" in captured.out
    assert "output root:" in captured.out


def test_run_normalization_pipeline_is_quiet_by_default(tmp_path: Path, capsys) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    (input_root / "0100_sample.LOG").write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "output"
    from mermaid_records.normalize_pipeline import run_normalization_pipeline

    run_normalization_pipeline(input_root, output_dir=output_dir)
    captured = capsys.readouterr()

    assert captured.out == ""
    assert captured.err == ""


def test_output_dir_resolves_from_mermaid_env(tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch) -> None:
    input_root = tmp_path / "inputs"
    mermaid_root = tmp_path / "mermaid"
    input_root.mkdir()
    mermaid_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    (input_root / "0100_sample.LOG").write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MERMAID", mermaid_root.as_posix())

    result = main(["normalize", "-i", str(input_root)])

    captured = capsys.readouterr()

    assert result == 0
    assert "NORMALIZATION SUMMARY" in captured.out
    assert (mermaid_root / "records" / "467.174-T-0100" / "log_operational_records.jsonl").exists()


def test_missing_output_dir_and_mermaid_env_errors_clearly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    (input_root / "0100_sample.LOG").write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("MERMAID", raising=False)

    with pytest.raises(SystemExit, match="--output-dir was not given and MERMAID is not set"):
        main(["normalize", "-i", str(input_root)])


def test_decoder_python_resolves_from_env_for_bin_runs(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "output"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")
    decoder = _write_decoder(tmp_path / "decoder.py", "decoded from env python")
    mermaid_root = tmp_path / "mermaid"
    database_root = mermaid_root / "database"
    database_root.mkdir(parents=True)
    (database_root / "Databases.json").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("MERMAID", mermaid_root.as_posix())
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_PYTHON", sys.executable)
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_SCRIPT", decoder.as_posix())

    result = main(["normalize", "-i", str(input_root), "-o", str(output_dir)])

    captured = capsys.readouterr()

    assert result == 0
    assert "bin files decoded=1" in captured.out
    rows = _jsonl_lines(output_dir / "0100" / "log_operational_records.jsonl")
    assert rows[0]["message"] == "decoded from env python"


def test_decoder_script_resolves_from_env_for_bin_runs(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "output"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")
    decoder = _write_decoder(tmp_path / "decoder.py", "decoded from env script")
    mermaid_root = tmp_path / "mermaid"
    database_root = mermaid_root / "database"
    database_root.mkdir(parents=True)
    (database_root / "Databases.json").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("MERMAID", mermaid_root.as_posix())
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_PYTHON", sys.executable)
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_SCRIPT", decoder.as_posix())

    result = main(["normalize", "-i", str(input_root), "-o", str(output_dir)])

    captured = capsys.readouterr()

    assert result == 0
    assert "bin files decoded=1" in captured.out
    rows = _jsonl_lines(output_dir / "0100" / "log_operational_records.jsonl")
    assert rows[0]["message"] == "decoded from env script"


def test_explicit_cli_decoder_args_override_env(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "output"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")
    env_decoder = _write_decoder(tmp_path / "decoder_env.py", "decoded from env")
    cli_decoder = _write_decoder(tmp_path / "decoder_cli.py", "decoded from cli")
    mermaid_root = tmp_path / "mermaid"
    database_root = mermaid_root / "database"
    database_root.mkdir(parents=True)
    (database_root / "Databases.json").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("MERMAID", mermaid_root.as_posix())
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_PYTHON", "/does/not/exist/python")
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_SCRIPT", env_decoder.as_posix())

    result = main(
        [
            "normalize",
            "-i",
            str(input_root),
            "-o",
            str(output_dir),
            "--decoder-python",
            sys.executable,
            "--decoder-script",
            str(cli_decoder),
        ]
    )

    captured = capsys.readouterr()

    assert result == 0
    assert "bin files decoded=1" in captured.out
    rows = _jsonl_lines(output_dir / "0100" / "log_operational_records.jsonl")
    assert rows[0]["message"] == "decoded from cli"


def test_bin_free_runs_do_not_require_decoder_env_or_args(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "output"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    (input_root / "0100_sample.LOG").write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("MERMAID_RECORDS_DECODER_PYTHON", raising=False)
    monkeypatch.delenv("MERMAID_RECORDS_DECODER_SCRIPT", raising=False)

    result = main(["normalize", "-i", str(input_root), "-o", str(output_dir)])

    captured = capsys.readouterr()

    assert result == 0
    assert "NORMALIZATION SUMMARY" in captured.out
    assert "bin files decoded=0" in captured.out
    assert (output_dir / "467.174-T-0100" / "log_operational_records.jsonl").exists()


def test_bin_runs_require_decoder_python_when_unresolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "output"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")
    decoder = _write_decoder(tmp_path / "decoder.py", "decoded")
    monkeypatch.delenv("MERMAID_RECORDS_DECODER_PYTHON", raising=False)
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_SCRIPT", decoder.as_posix())

    with pytest.raises(SystemExit, match="Provide --decoder-python or set MERMAID_RECORDS_DECODER_PYTHON"):
        main(["normalize", "-i", str(input_root), "-o", str(output_dir)])


def test_bin_runs_require_decoder_script_when_unresolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "output"
    input_root.mkdir()
    (input_root / "0100_sample.BIN").write_bytes(b"raw-bin")
    monkeypatch.setenv("MERMAID_RECORDS_DECODER_PYTHON", sys.executable)
    monkeypatch.delenv("MERMAID_RECORDS_DECODER_SCRIPT", raising=False)

    with pytest.raises(SystemExit, match="Provide --decoder-script or set MERMAID_RECORDS_DECODER_SCRIPT"):
        main(["normalize", "-i", str(input_root), "-o", str(output_dir)])


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
    log = workdir / "0100_sample.LOG"
    log.write_text("1700000000:[MAIN  ,0007]{message}\\n", encoding="utf-8")
    return [path]
""",
        encoding="utf-8",
    )
    return path


def _jsonl_lines(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
