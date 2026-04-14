# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path

from mermaid_records.normalize_log import write_log_jsonl_prototypes


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
    assert summary.ascent_request_records == 0
    assert summary.gps_records == 0
    assert summary.transmission_records == 2
    assert summary.measurement_records == 2
    assert summary.unclassified_records == 2

    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")
    acquisition_records = _read_jsonl(output_dir / "log_acquisition_records.jsonl")
    ascent_request_records = _read_jsonl(
        output_dir / "log_ascent_request_records.jsonl"
    )
    gps_records = _read_jsonl(output_dir / "log_gps_records.jsonl")
    transmission_records = _read_jsonl(output_dir / "log_transmission_records.jsonl")
    measurement_records = _read_jsonl(output_dir / "log_measurement_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert len(operational_records) == 6
    assert acquisition_records == []
    assert ascent_request_records == []
    assert gps_records == []
    assert len(transmission_records) == 2
    assert len(measurement_records) == 2
    assert len(unclassified_records) == 2

    assert operational_records[0]["message_kind"] == "upload"
    assert operational_records[2]["message_kind"] == "measurement"
    assert operational_records[4]["severity"] == "warn"
    assert operational_records[0]["record_time"] == "2023-11-14T22:13:20"
    assert operational_records[0]["log_epoch_time"] == "1700000000"
    assert "time" not in operational_records[0]

    assert transmission_records[1]["referenced_artifact"] == "0100/AAAA0001.MER"
    assert transmission_records[1]["rate_bytes_per_s"] == 83
    assert transmission_records[1]["record_time"] == "2023-11-14T22:13:21"
    assert transmission_records[1]["log_epoch_time"] == "1700000001"
    assert "time" not in transmission_records[1]

    assert measurement_records[0]["measurement_kind"] == "pressure_temperature"
    assert measurement_records[1]["measurement_kind"] == "pump_duration"
    assert measurement_records[0]["record_time"] == "2023-11-14T22:13:22"
    assert measurement_records[0]["log_epoch_time"] == "1700000002"
    assert "time" not in measurement_records[0]

    assert all(
        record["unclassified_reason"] == "no_family_match"
        for record in unclassified_records
    )
    assert unclassified_records[0]["record_time"] == "2023-11-14T22:13:24"
    assert unclassified_records[0]["log_epoch_time"] == "1700000004"
    assert "time" not in unclassified_records[0]
    assert {
        record["message"] for record in unclassified_records
    } == {"<WARN>timeout", "buoy 467.174-T-0100"}
    assert all(record["float_id"] == "0100" for record in operational_records)


def test_write_log_jsonl_prototypes_accepts_canonical_float_id_override(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_sample.LOG"
    log_path.write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    write_log_jsonl_prototypes([log_path], output_dir, float_id="T0100")
    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")

    assert operational_records[0]["float_id"] == "T0100"


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
    measurement_records = _read_jsonl(output_dir / "log_measurement_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert summary.total_records == 2
    assert summary.measurement_records == 2
    assert summary.ascent_request_records == 0
    assert summary.gps_records == 0
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
    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")
    acquisition_records = _read_jsonl(output_dir / "log_acquisition_records.jsonl")
    gps_records = _read_jsonl(output_dir / "log_gps_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert summary.total_records == 5
    assert summary.operational_records == 5
    assert summary.acquisition_records == 4
    assert summary.ascent_request_records == 0
    assert summary.gps_records == 1
    assert summary.unclassified_records == 0
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
    assert acquisition_records[0]["record_time"] == "2023-11-14T22:13:20"
    assert acquisition_records[0]["log_epoch_time"] == "1700000000"
    assert "time" not in acquisition_records[0]
    assert gps_records[0]["gps_record_kind"] == "fix_attempt"
    assert gps_records[0]["raw_values"] is None
    assert gps_records[0]["record_time"] == "2023-11-14T22:13:24"
    assert gps_records[0]["log_epoch_time"] == "1700000004"
    assert "time" not in gps_records[0]
    assert unclassified_records == []


def test_write_log_jsonl_prototypes_emits_ascent_request_records(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_ascent.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[MRMAID,0583]ascent request accepted",
                "1700000001:[MRMAID,0005]ascent request rejected",
                "1700000002:[SURF  ,0022]GPS fix...",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    summary = write_log_jsonl_prototypes([log_path], output_dir)
    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")
    ascent_request_records = _read_jsonl(
        output_dir / "log_ascent_request_records.jsonl"
    )
    gps_records = _read_jsonl(output_dir / "log_gps_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert summary.total_records == 3
    assert summary.operational_records == 3
    assert summary.ascent_request_records == 2
    assert summary.gps_records == 1
    assert summary.ascent_request_state_counts == {
        "accepted": 1,
        "rejected": 1,
    }
    assert len(ascent_request_records) == 2
    assert {record["ascent_request_state"] for record in ascent_request_records} == {
        "accepted",
        "rejected",
    }
    assert ascent_request_records[0]["record_time"] == "2023-11-14T22:13:20"
    assert ascent_request_records[0]["log_epoch_time"] == "1700000000"
    assert "time" not in ascent_request_records[0]
    assert operational_records[0]["message_kind"] == "status"
    assert operational_records[1]["message_kind"] == "status"
    assert gps_records[0]["gps_record_kind"] == "fix_attempt"
    assert unclassified_records == []


def test_write_log_jsonl_prototypes_emits_gps_records(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_gps.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[SURF  ,0022]GPS fix...",
                "1700000001:[SURF  ,0082]N35deg19.262mn, E139deg39.043mn",
                "1700000002:[SURF  ,0084]hdop 0.820, vdop 1.180",
                "1700000003:[MRMAID,0052]$GPSACK:+0,+0,+0,+0,+0,+0,-30;",
                "1700000004:[MRMAID,0052]$GPSOFF:3686327;",
                "1700000005:[MAIN  ,0007]buoy 467.174-T-0100",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    summary = write_log_jsonl_prototypes([log_path], output_dir)
    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")
    gps_records = _read_jsonl(output_dir / "log_gps_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert summary.total_records == 6
    assert summary.gps_records == 5
    assert summary.gps_record_kind_counts == {
        "dop": 1,
        "fix_attempt": 1,
        "fix_position": 1,
        "gps_ack": 1,
        "gps_off": 1,
    }
    assert summary.unclassified_records == 1
    assert len(gps_records) == 5
    assert [record["message_kind"] for record in operational_records[:5]] == [
        "gps",
        "gps",
        "gps",
        "gps",
        "gps",
    ]
    assert gps_records[0]["gps_record_kind"] == "fix_attempt"
    assert gps_records[0]["raw_values"] is None
    assert gps_records[1]["raw_values"] == {
        "latitude": "N35deg19.262mn",
        "longitude": "E139deg39.043mn",
    }
    assert gps_records[2]["raw_values"] == {"hdop": "0.820", "vdop": "1.180"}
    assert gps_records[3]["raw_values"] == {"gpsack": "+0,+0,+0,+0,+0,+0,-30"}
    assert gps_records[4]["raw_values"] == {"gpsoff": "3686327"}
    assert gps_records[2]["record_time"] == "2023-11-14T22:13:22"
    assert gps_records[2]["log_epoch_time"] == "1700000002"
    assert "time" not in gps_records[2]
    assert [record["message"] for record in unclassified_records] == [
        "buoy 467.174-T-0100"
    ]


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
