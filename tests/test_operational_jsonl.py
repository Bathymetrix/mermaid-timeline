# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path

from mermaid_timeline.operational_jsonl import write_log_jsonl_prototypes


def test_write_log_jsonl_prototypes_preserves_unclassified_records(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_sample.LOG"
    log_path.write_text(
        "\n".join(
            [
                '1700000000:[UPLOAD,0248]Upload data files...',
                '1700000001:[UPLOAD,0231]"0100/AAAA0001.MER" uploaded at 83bytes/s',
                "1700000002:[PRESS ,0038]P+20179mbar,T+32767mdegC",
                "1700000003:[PUMP  ,0016]pump during 30000ms",
                "1700000004:[SURF  ,0071]<WARN>timeout",
                "1700000005:[MAIN  ,0007]buoy 467.174-T-0100",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    summary = write_log_jsonl_prototypes([log_path], output_dir)

    assert summary.total_records == 6
    assert summary.operational_records == 6
    assert summary.acquisition_records == 0
    assert summary.transmission_records == 2
    assert summary.measurement_records == 2
    assert summary.unclassified_records == 2

    operational_records = _read_jsonl(output_dir / "operational_records.jsonl")
    acquisition_records = _read_jsonl(output_dir / "acquisition_records.jsonl")
    transmission_records = _read_jsonl(output_dir / "transmission_records.jsonl")
    measurement_records = _read_jsonl(output_dir / "measurement_records.jsonl")
    unclassified_records = _read_jsonl(
        output_dir / "unclassified_operational_records.jsonl"
    )

    assert len(operational_records) == 6
    assert acquisition_records == []
    assert len(transmission_records) == 2
    assert len(measurement_records) == 2
    assert len(unclassified_records) == 2

    assert operational_records[0]["message_kind"] == "upload"
    assert operational_records[2]["message_kind"] == "measurement"
    assert operational_records[4]["severity"] == "warn"

    assert transmission_records[1]["referenced_artifact"] == "0100/AAAA0001.MER"
    assert transmission_records[1]["rate_bytes_per_s"] == 83

    assert measurement_records[0]["measurement_kind"] == "pressure_temperature"
    assert measurement_records[1]["measurement_kind"] == "pump_duration"

    assert all(
        record["unclassified_reason"] == "no_family_match"
        for record in unclassified_records
    )
    assert {
        record["message"] for record in unclassified_records
    } == {"<WARN>timeout", "buoy 467.174-T-0100"}


def test_write_log_jsonl_prototypes_classifies_legacy_pump_and_outflow_lines(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "06_sample.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[PUMP  ,368]during 900000ms",
                "1700000001:[PUMP  ,0378]Outflow calculated : 2711",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    summary = write_log_jsonl_prototypes([log_path], output_dir)
    measurement_records = _read_jsonl(output_dir / "measurement_records.jsonl")
    unclassified_records = _read_jsonl(
        output_dir / "unclassified_operational_records.jsonl"
    )

    assert summary.total_records == 2
    assert summary.measurement_records == 2
    assert summary.unclassified_records == 0
    assert [record["measurement_kind"] for record in measurement_records] == [
        "pump_duration",
        "outflow",
    ]
    assert unclassified_records == []


def test_write_log_jsonl_prototypes_emits_acquisition_records(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_acq.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[MRMAID,0002]acq started",
                "1700000001:[MRMAID,0003]acq stopped",
                "1700000002:[MRMAID,0184]acq already started",
                "1700000003:[MRMAID,0185]acq already stopped",
                "1700000004:[SURF  ,0022]GPS fix...",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    summary = write_log_jsonl_prototypes([log_path], output_dir)
    operational_records = _read_jsonl(output_dir / "operational_records.jsonl")
    acquisition_records = _read_jsonl(output_dir / "acquisition_records.jsonl")
    unclassified_records = _read_jsonl(
        output_dir / "unclassified_operational_records.jsonl"
    )

    assert summary.total_records == 5
    assert summary.operational_records == 5
    assert summary.acquisition_records == 4
    assert summary.unclassified_records == 1
    assert summary.acquisition_state_counts == {"started": 2, "stopped": 2}
    assert summary.acquisition_evidence_kind_counts == {
        "transition": 2,
        "assertion": 2,
    }

    assert len(operational_records) == 5
    assert [record["message_kind"] for record in operational_records[:4]] == [
        "acquisition",
        "acquisition",
        "acquisition",
        "acquisition",
    ]
    assert {(record["acquisition_state"], record["acquisition_evidence_kind"]) for record in acquisition_records} == {
        ("started", "transition"),
        ("stopped", "transition"),
        ("started", "assertion"),
        ("stopped", "assertion"),
    }
    assert [record["message"] for record in unclassified_records] == ["GPS fix..."]


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
