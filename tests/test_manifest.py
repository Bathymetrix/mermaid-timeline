# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from mermaid_records.bin2log import Bin2LogConfig
import mermaid_records.normalize_log as normalize_log_module
import mermaid_records.normalize_mer as normalize_mer_module
from mermaid_records.normalize_pipeline import run_normalization_pipeline


def test_stateful_run_writes_per_instrument_outputs_and_manifests(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "452.020-P-06.vit").write_text("", encoding="utf-8")
    _write_log(input_root / "06_first.LOG", "first")
    _write_mer(input_root / "06_first.MER")

    output_root = tmp_path / "output"
    summary = run_normalization_pipeline(input_root, output_dir=output_root)

    instrument_dir = output_root / "452.020-P-06"
    latest = _read_json(instrument_dir / "manifests" / "latest.json")
    run_json = _read_json(instrument_dir / latest["run_manifest"])
    source_state = _read_json(instrument_dir / latest["source_state_manifest"])
    diff_rows = _jsonl_lines(instrument_dir / "manifests" / "runs" / latest["run_id"] / "input_file_diffs.jsonl")

    assert summary.mode == "stateful"
    assert [item.instrument_id for item in summary.processed_instruments] == ["P0006"]
    assert (instrument_dir / "log_operational_records.jsonl").exists()
    assert (instrument_dir / "mer_environment_records.jsonl").exists()
    assert run_json["status"] == "success"
    assert source_state["input_root"] == input_root.as_posix()
    assert source_state["normalization_version"] == "0.1.0"
    assert {item["source_kind"] for item in source_state["raw_sources"]} == {"log", "mer"}
    assert {item["change_kind"] for item in diff_rows} == {"new"}
    assert all(item["run_id"] == latest["run_id"] for item in diff_rows)
    assert {item["source_file"] for item in diff_rows} == {"06_first.LOG", "06_first.MER"}
    assert {item["instrument_id"] for item in diff_rows} == {"P0006"}
    assert list(diff_rows[0]) == [
        "source_file",
        "source_kind",
        "instrument_id",
        "previous_exists",
        "current_exists",
        "previous_size_bytes",
        "current_size_bytes",
        "previous_hash",
        "current_hash",
        "change_kind",
        "decoder_state_changed",
        "run_id",
    ]


def test_stateful_append_path_appends_only_new_files(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    _write_log(input_root / "0100_first.LOG", "first")
    output_root = tmp_path / "output"

    run_normalization_pipeline(input_root, output_dir=output_root)
    _write_log(input_root / "0100_second.LOG", "second")
    summary = run_normalization_pipeline(input_root, output_dir=output_root)

    instrument_summary = summary.processed_instruments[0]
    operational_lines = _jsonl_lines(output_root / "467.174-T-0100" / "log_operational_records.jsonl")

    assert instrument_summary.log_action == "append"
    assert len(operational_lines) == 2
    assert operational_lines[0]["message"] == "first"
    assert operational_lines[1]["message"] == "second"


def test_stateful_second_run_with_no_raw_source_changes_is_noop(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    _write_log(input_root / "0100_first.LOG", "first")
    _write_mer(input_root / "0100_first.MER")
    output_root = tmp_path / "output"

    run_normalization_pipeline(input_root, output_dir=output_root)
    summary = run_normalization_pipeline(input_root, output_dir=output_root)

    instrument_summary = summary.processed_instruments[0]
    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    diff_rows = _jsonl_lines(
        output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"] / "input_file_diffs.jsonl"
    )

    assert instrument_summary.log_action == "noop"
    assert instrument_summary.mer_action == "noop"
    assert {row["change_kind"] for row in diff_rows} == {"unchanged"}


def test_stateful_append_path_appends_only_new_mer_files(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    _write_mer(input_root / "0100_first.MER")
    output_root = tmp_path / "output"

    run_normalization_pipeline(input_root, output_dir=output_root)
    _write_second_mer(input_root / "0100_second.MER")
    summary = run_normalization_pipeline(input_root, output_dir=output_root)

    instrument_summary = summary.processed_instruments[0]
    data_lines = _jsonl_lines(output_root / "467.174-T-0100" / "mer_data_records.jsonl")

    assert instrument_summary.mer_action == "append"
    assert len(data_lines) == 2
    assert data_lines[0]["fname"] == "2024-02-07T22_47_22.000000"
    assert data_lines[1]["fname"] == "2024-02-08T01_02_03.000000"


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

    assert summary.processed_instruments[0].log_action == "rewrite"
    assert len(lines_after_change) == 1
    assert lines_after_change[0]["message"] == "first changed"

    log_path.unlink()
    summary = run_normalization_pipeline(input_root, output_dir=output_root)
    pruned_lines = _jsonl_lines(output_root / "467.174-T-0100" / "state" / "pruned_records.jsonl")

    assert summary.processed_instruments[0].log_action == "rewrite"
    assert not (output_root / "467.174-T-0100" / "log_operational_records.jsonl").exists()
    assert pruned_lines[-1]["source_file"] == log_path.as_posix()
    assert pruned_lines[-1]["source_kind"] == "log"


def test_stateful_rewrite_and_prune_on_changed_or_removed_mer_source(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    mer_path = input_root / "0100_first.MER"
    _write_mer(mer_path)
    output_root = tmp_path / "output"

    run_normalization_pipeline(input_root, output_dir=output_root)
    _write_second_mer(mer_path)
    summary = run_normalization_pipeline(input_root, output_dir=output_root)
    data_lines_after_change = _jsonl_lines(output_root / "467.174-T-0100" / "mer_data_records.jsonl")

    assert summary.processed_instruments[0].mer_action == "rewrite"
    assert len(data_lines_after_change) == 1
    assert data_lines_after_change[0]["fname"] == "2024-02-08T01_02_03.000000"

    mer_path.unlink()
    summary = run_normalization_pipeline(input_root, output_dir=output_root)
    pruned_lines = _jsonl_lines(output_root / "467.174-T-0100" / "state" / "pruned_records.jsonl")

    assert summary.processed_instruments[0].mer_action == "rewrite"
    assert not (output_root / "467.174-T-0100" / "mer_data_records.jsonl").exists()
    assert pruned_lines[-1]["source_file"] == mer_path.as_posix()
    assert pruned_lines[-1]["source_kind"] == "mer"


def test_decoder_state_invalidates_only_bin_dependent_instrument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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

    by_instrument = {item.instrument_id: item for item in summary.processed_instruments}
    bin_lines = _jsonl_lines(output_root / "0100" / "log_operational_records.jsonl")
    log_only_after = (output_root / "0200" / "log_operational_records.jsonl").read_text(encoding="utf-8")

    assert by_instrument["0100"].decoder_state_invalidated is True
    assert by_instrument["0100"].log_action == "rewrite"
    assert by_instrument["0200"].decoder_state_invalidated is False
    assert by_instrument["0200"].log_action == "noop"
    assert bin_lines[0]["message"] == "decoded b"
    assert log_only_before == log_only_after
    latest = _read_json(output_root / "0100" / "manifests" / "latest.json")
    diff_rows = _jsonl_lines(output_root / "0100" / "manifests" / "runs" / latest["run_id"] / "input_file_diffs.jsonl")
    assert diff_rows[0]["change_kind"] == "unchanged"
    assert diff_rows[0]["decoder_state_changed"] is True
    assert diff_rows[0]["source_file"] == "0100_first.BIN"


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


def test_stateless_dry_run_is_side_effect_free(tmp_path: Path) -> None:
    log_path = tmp_path / "0100_first.LOG"
    _write_log(log_path, "first")
    output_root = tmp_path / "output"

    summary = run_normalization_pipeline(
        output_dir=output_root,
        input_files=[log_path],
        dry_run=True,
    )
    payload = summary.to_dict()

    assert summary.mode == "stateless"
    assert payload["instruments"][0]["families"]["log"]["action"] == "append"
    assert payload["instruments"][0]["counts"]["new"] == 1
    assert not output_root.exists()


def test_dry_run_is_side_effect_free_and_reports_file_diffs(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    log_path = input_root / "0100_first.LOG"
    mer_path = input_root / "0100_first.MER"
    _write_log(log_path, "first")
    _write_mer(mer_path)

    output_root = tmp_path / "output"
    run_normalization_pipeline(input_root, output_dir=output_root)

    _write_log(log_path, "first changed")
    _write_log(input_root / "0100_second.LOG", "second")
    mer_path.unlink()

    latest_before = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    runs_root = output_root / "467.174-T-0100" / "manifests" / "runs"
    run_ids_before = sorted(path.name for path in runs_root.iterdir() if path.is_dir())
    summary = run_normalization_pipeline(input_root, output_dir=output_root, dry_run=True)
    payload = summary.to_dict()

    assert summary.mode == "stateful"
    assert _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json") == latest_before
    assert sorted(path.name for path in runs_root.iterdir() if path.is_dir()) == run_ids_before
    assert not (output_root / "467.174-T-0100" / "state" / "pruned_records.jsonl").exists()
    instrument_payload = payload["instruments"][0]
    assert instrument_payload["counts"] == {
        "total": 3,
        "new": 1,
        "changed": 1,
        "removed": 1,
        "unchanged": 0,
    }
    assert instrument_payload["families"]["log"]["action"] == "rewrite"
    assert instrument_payload["families"]["mer"]["action"] == "rewrite"
    assert {row["change_kind"] for row in instrument_payload["families"]["log"]["file_diffs"]} == {"new", "changed"}
    assert {row["change_kind"] for row in instrument_payload["families"]["mer"]["file_diffs"]} == {"removed"}


def test_bin_decode_failure_reports_offending_source_paths(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    bin_path = input_root / "0100_first.BIN"
    bin_path.write_bytes(b"raw-bin")
    decoder = tmp_path / "decoder_fail.py"
    decoder.write_text(
        """
def database_update(_arg):
    print("Update Databases")

def concatenate_files(path):
    return [path]

def concatenate_rbr_files(path):
    return [path]

def decrypt_all(path):
    raise RuntimeError("decoder boom")
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Error while decoding BIN source\\(s\\)") as excinfo:
        run_normalization_pipeline(
            input_root,
            output_dir=tmp_path / "output",
            config=Bin2LogConfig(
                python_executable=Path(sys.executable),
                decoder_script=decoder,
            ),
        )

    assert bin_path.as_posix() in str(excinfo.value)


def test_stateful_logs_malformed_log_lines_and_continues(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    log_path = input_root / "0100_malformed.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[MAIN  ,0007]first",
                "[DIVING,15",
                "1700000001:[MAIN  ,0007]second",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_root = tmp_path / "output"
    run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    run_dir = output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"]
    malformed_rows = _jsonl_lines(run_dir / "malformed_log_lines.jsonl")
    operational_rows = _jsonl_lines(output_root / "467.174-T-0100" / "log_operational_records.jsonl")

    assert [row["message"] for row in operational_rows] == ["first", "second"]
    assert malformed_rows == [
        {
            "error": "line does not match expected LOG pattern",
            "instrument_id": "T0100",
            "line_number": 2,
            "raw_line": "[DIVING,15",
            "run_id": latest["run_id"],
            "source_file": log_path.as_posix(),
        }
    ]
    assert _jsonl_lines(run_dir / "skipped_log_files.jsonl") == []


def test_stateful_records_skipped_log_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    log_path = input_root / "0100_broken.LOG"
    _write_log(log_path, "first")

    def _raise_unreadable(path: Path, *, on_malformed_line=None):
        raise OSError("simulated unreadable log")
        yield  # pragma: no cover

    monkeypatch.setattr(normalize_log_module, "iter_operational_log_entries", _raise_unreadable)

    output_root = tmp_path / "output"
    run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    run_dir = output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"]
    skipped_rows = _jsonl_lines(run_dir / "skipped_log_files.jsonl")

    assert skipped_rows == [
        {
            "error": "simulated unreadable log",
            "instrument_id": "T0100",
            "run_id": latest["run_id"],
            "source_file": log_path.as_posix(),
            "skipped_at": skipped_rows[0]["skipped_at"],
        }
    ]
    assert _jsonl_lines(run_dir / "malformed_log_lines.jsonl") == []


def test_stateful_logs_malformed_mer_blocks_and_continues(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    mer_path = input_root / "0100_malformed.MER"
    mer_path.write_bytes(
        (
            "<ENVIRONMENT>\n"
            "\t<BOARD 452116600-A0 />\n"
            "</ENVIRONMENT>\n"
            "<PARAMETERS>\n"
            "\t<MISC UPLOAD_MAX=100kB />\n"
            "</PARAMETERS>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-07T22:47:22 FNAME=bad.000000 SMP_OFFSET=1 TRUE_FS=40.0 />\n"
            "\t<DATA>BAD</DATA>\n"
            "</EVENT>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-08T01:02:03 FNAME=good.000000 SMP_OFFSET=2 TRUE_FS=40.0 />\n"
            "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            "STAGES=5 NORMALIZED=YES LENGTH=12 />\n"
            "\t<DATA>GOOD</DATA>\n"
            "</EVENT>\n"
        ).encode("ascii")
    )

    output_root = tmp_path / "output"
    run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    run_dir = output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"]
    malformed_rows = _jsonl_lines(run_dir / "malformed_mer_blocks.jsonl")
    data_rows = _jsonl_lines(output_root / "467.174-T-0100" / "mer_data_records.jsonl")

    assert len(data_rows) == 1
    assert data_rows[0]["fname"] == "good.000000"
    assert malformed_rows == [
        {
            "block_index": 0,
            "block_kind": "event_format",
            "error": "missing FORMAT tag",
            "instrument_id": "T0100",
            "raw_block": malformed_rows[0]["raw_block"],
            "run_id": latest["run_id"],
            "source_file": mer_path.as_posix(),
        }
    ]
    assert "<EVENT>" in malformed_rows[0]["raw_block"]
    assert _jsonl_lines(run_dir / "skipped_mer_files.jsonl") == []


def test_stateful_logs_incomplete_mer_data_block_and_continues(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    mer_path = input_root / "0100_incomplete_data.MER"
    mer_path.write_bytes(
        (
            "<ENVIRONMENT>\n"
            "\t<BOARD 452116600-A0 />\n"
            "</ENVIRONMENT>\n"
            "<PARAMETERS>\n"
            "\t<MISC UPLOAD_MAX=100kB />\n"
            "</PARAMETERS>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-07T22:47:22 FNAME=bad.000000 SMP_OFFSET=1 TRUE_FS=40.0 />\n"
            "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            "STAGES=5 NORMALIZED=YES LENGTH=12 />\n"
            "\t<DATA>\n\rABCDEF\n"
            "</EVENT>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-08T01:02:03 FNAME=good.000000 SMP_OFFSET=2 TRUE_FS=40.0 />\n"
            "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            "STAGES=5 NORMALIZED=YES LENGTH=3 />\n"
            "\t<DATA>GOOD</DATA>\n"
            "</EVENT>\n"
        ).encode("ascii")
    )

    output_root = tmp_path / "output"
    run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    run_dir = output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"]
    malformed_rows = _jsonl_lines(run_dir / "malformed_mer_blocks.jsonl")
    data_rows = _jsonl_lines(output_root / "467.174-T-0100" / "mer_data_records.jsonl")

    assert len(data_rows) == 1
    assert data_rows[0]["fname"] == "good.000000"
    assert malformed_rows == [
        {
            "block_index": 0,
            "block_kind": "event_data",
            "error": "incomplete DATA block: missing </DATA>",
            "instrument_id": "T0100",
            "raw_block": malformed_rows[0]["raw_block"],
            "run_id": latest["run_id"],
            "source_file": mer_path.as_posix(),
        }
    ]
    assert "<DATA>" in malformed_rows[0]["raw_block"]


def test_stateful_records_skipped_mer_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    mer_path = input_root / "0100_broken.MER"
    _write_mer(mer_path)

    def _raise_unreadable(path: Path, *, on_malformed_block=None):
        raise OSError("simulated unreadable mer")

    monkeypatch.setattr(normalize_mer_module, "parse_mer_file_recoverable", _raise_unreadable)

    output_root = tmp_path / "output"
    run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    run_dir = output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"]
    skipped_rows = _jsonl_lines(run_dir / "skipped_mer_files.jsonl")

    assert skipped_rows == [
        {
            "error": "simulated unreadable mer",
            "instrument_id": "T0100",
            "run_id": latest["run_id"],
            "source_file": mer_path.as_posix(),
            "skipped_at": skipped_rows[0]["skipped_at"],
        }
    ]
    assert _jsonl_lines(run_dir / "malformed_mer_blocks.jsonl") == []


def test_stateful_skips_hopelessly_broken_mer_file(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    mer_path = input_root / "0100_hopeless.MER"
    mer_path.write_bytes(b"this is not a mer file at all")

    output_root = tmp_path / "output"
    run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    run_dir = output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"]
    skipped_rows = _jsonl_lines(run_dir / "skipped_mer_files.jsonl")

    assert skipped_rows == [
        {
            "error": (
                "MER structure unreadable: no recoverable ENVIRONMENT, PARAMETERS, "
                "or EVENT content"
            ),
            "instrument_id": "T0100",
            "run_id": latest["run_id"],
            "source_file": mer_path.as_posix(),
            "skipped_at": skipped_rows[0]["skipped_at"],
        }
    ]
    assert _jsonl_lines(run_dir / "malformed_mer_blocks.jsonl") == []


def test_stateless_malformed_log_recovery_writes_no_manifests(tmp_path: Path) -> None:
    log_path = tmp_path / "0100_malformed.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[MAIN  ,0007]first",
                "[DIVING,15",
                "1700000001:[MAIN  ,0007]second",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_root = tmp_path / "output"
    run_normalization_pipeline(output_dir=output_root, input_files=[log_path])

    assert not (output_root / "0100" / "manifests").exists()
    operational_rows = _jsonl_lines(output_root / "0100" / "log_operational_records.jsonl")
    assert [row["message"] for row in operational_rows] == ["first", "second"]


def test_first_run_diff_semantics_treat_previous_state_as_empty(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    _write_log(input_root / "0100_first.LOG", "first")
    output_root = tmp_path / "output"

    run_normalization_pipeline(input_root, output_dir=output_root)

    latest = _read_json(output_root / "467.174-T-0100" / "manifests" / "latest.json")
    diff_rows = _jsonl_lines(
        output_root / "467.174-T-0100" / "manifests" / "runs" / latest["run_id"] / "input_file_diffs.jsonl"
    )

    assert len(diff_rows) == 1
    assert diff_rows[0]["source_file"] == "0100_first.LOG"
    assert diff_rows[0]["previous_exists"] is False
    assert diff_rows[0]["current_exists"] is True
    assert diff_rows[0]["previous_size_bytes"] == 0
    assert diff_rows[0]["previous_hash"] is None
    assert diff_rows[0]["change_kind"] == "new"


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


def _write_second_mer(path: Path) -> None:
    path.write_bytes(
        (
            "<ENVIRONMENT>\n"
            "\t<BOARD 452116600-A0 />\n"
            "</ENVIRONMENT>\n"
            "<PARAMETERS>\n"
            "\t<MISC UPLOAD_MAX=200kB />\n"
            "</PARAMETERS>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-08T01:02:03 FNAME=2024-02-08T01_02_03.000000 "
            "SMP_OFFSET=614055 TRUE_FS=40.014107 />\n"
            "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            "STAGES=5 NORMALIZED=YES LENGTH=2048 />\n"
            "\t<DATA>DEFG</DATA>\n"
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
