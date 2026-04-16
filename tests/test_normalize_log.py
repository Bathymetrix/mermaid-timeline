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
    assert summary.parameter_records == 0
    assert summary.transmission_records == 2
    assert summary.measurement_records == 2
    assert summary.unclassified_records == 2

    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")
    acquisition_records = _read_jsonl(output_dir / "log_acquisition_records.jsonl")
    ascent_request_records = _read_jsonl(
        output_dir / "log_ascent_request_records.jsonl"
    )
    gps_records = _read_jsonl(output_dir / "log_gps_records.jsonl")
    parameter_records = _read_jsonl(output_dir / "log_parameter_records.jsonl")
    transmission_records = _read_jsonl(output_dir / "log_transmission_records.jsonl")
    measurement_records = _read_jsonl(output_dir / "log_measurement_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert len(operational_records) == 6
    assert acquisition_records == []
    assert ascent_request_records == []
    assert gps_records == []
    assert parameter_records == []
    assert len(transmission_records) == 2
    assert len(measurement_records) == 2
    assert len(unclassified_records) == 2

    assert operational_records[0]["message_kind"] == "upload"
    assert operational_records[2]["message_kind"] == "measurement"
    assert operational_records[4]["severity"] == "warn"
    assert list(operational_records[0]) == [
        "instrument_id",
        "source_file",
        "source_container",
        "record_time",
        "log_epoch_time",
        "subsystem",
        "code",
        "message",
        "severity",
        "message_kind",
        "raw_line",
    ]
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
    assert all(record["instrument_id"] == "0100" for record in operational_records)


def test_write_log_jsonl_prototypes_accepts_canonical_instrument_id_override(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_sample.LOG"
    log_path.write_text(
        "1700000000:[MAIN  ,0007]buoy 467.174-T-0100\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    write_log_jsonl_prototypes([log_path], output_dir, instrument_id="T0100")
    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")

    assert operational_records[0]["instrument_id"] == "T0100"


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
    assert summary.parameter_records == 0
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
    assert summary.parameter_records == 0
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
    assert summary.parameter_records == 0
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


def test_write_log_jsonl_prototypes_groups_parameter_block_into_one_episode(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_params.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[MAIN  ,0593]internal pressure 85448Pa",
                "1700000001:    bypass 20000ms 120000ms (10000ms 200000ms stored)",
                "1700000001:    valve 60000ms 12750 (60000ms 12750 stored)",
                "1700000001:    stage[0] 150000mbar (+/-5000mbar) 60000s (<60000s)",
                "1700000001:    stage[1] 150000mbar (+/-5000mbar) 648000s (<708000s)",
                "1700000002:[MAIN  ,0621]turn off bluetooth",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    malformed_log_lines: list[dict[str, object]] = []
    summary = write_log_jsonl_prototypes(
        [log_path],
        output_dir,
        run_id="run-1",
        malformed_log_lines=malformed_log_lines,
    )
    parameter_records = _read_jsonl(output_dir / "log_parameter_records.jsonl")

    assert summary.total_records == 3
    assert summary.operational_records == 2
    assert summary.parameter_records == 1
    assert len(parameter_records) == 1
    assert malformed_log_lines == []

    assert parameter_records[0] == {
        "instrument_id": "0100",
        "source_file": log_path.as_posix(),
        "episode_index": 0,
        "line_start_index": 2,
        "line_end_index": 5,
        "start_record_time": "2023-11-14T22:13:21",
        "end_record_time": "2023-11-14T22:13:21",
        "start_log_epoch_time": "1700000001",
        "end_log_epoch_time": "1700000001",
        "raw_lines": [
            "1700000001:    bypass 20000ms 120000ms (10000ms 200000ms stored)",
            "1700000001:    valve 60000ms 12750 (60000ms 12750 stored)",
            "1700000001:    stage[0] 150000mbar (+/-5000mbar) 60000s (<60000s)",
            "1700000001:    stage[1] 150000mbar (+/-5000mbar) 648000s (<708000s)",
        ],
    }


def test_write_log_jsonl_prototypes_stops_parameter_episode_at_explicit_boundaries(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0100_parameter_boundaries.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[MAIN  ,0593]internal pressure 85448Pa",
                "1700000001:    bypass 20000ms 120000ms (10000ms 200000ms stored)",
                "1700000001:    valve 60000ms 12750 (60000ms 12750 stored)",
                "1700000002:[SURF  ,0071]<WARN>timeout",
                "1700000003:    pump 60000ms 30% 10750 80% (60000ms 30% 10750 80% stored)",
                "1700000003:    rate 2mbar/s (2mbar/s stored)",
                "1700000004:*** switching to 0100/NEXT.LOG ***",
                "1700000005:    surface 500mbar (300mbar stored)",
                "1700000005:    ascent 8mbar/s (8mbar/s stored)",
                "1700000006:Command list",
                "1700000007:[MAIN  ,0007]buoy 467.174-T-0100",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    malformed_log_lines: list[dict[str, object]] = []
    summary = write_log_jsonl_prototypes(
        [log_path],
        output_dir,
        run_id="run-2",
        malformed_log_lines=malformed_log_lines,
    )
    parameter_records = _read_jsonl(output_dir / "log_parameter_records.jsonl")
    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")

    assert summary.total_records == 6
    assert summary.operational_records == 3
    assert summary.parameter_records == 3
    assert [record["episode_index"] for record in parameter_records] == [0, 1, 2]
    assert [record["line_start_index"] for record in parameter_records] == [2, 5, 8]
    assert [record["line_end_index"] for record in parameter_records] == [3, 6, 9]
    assert [record["raw_lines"] for record in parameter_records] == [
        [
            "1700000001:    bypass 20000ms 120000ms (10000ms 200000ms stored)",
            "1700000001:    valve 60000ms 12750 (60000ms 12750 stored)",
        ],
        [
            "1700000003:    pump 60000ms 30% 10750 80% (60000ms 30% 10750 80% stored)",
            "1700000003:    rate 2mbar/s (2mbar/s stored)",
        ],
        [
            "1700000005:    surface 500mbar (300mbar stored)",
            "1700000005:    ascent 8mbar/s (8mbar/s stored)",
        ],
    ]
    assert [record["message"] for record in operational_records] == [
        "internal pressure 85448Pa",
        "<WARN>timeout",
        "buoy 467.174-T-0100",
    ]
    assert malformed_log_lines == [
        {
            "run_id": "run-2",
            "instrument_id": "0100",
            "source_file": log_path.as_posix(),
            "line_number": 7,
            "raw_line": "1700000004:*** switching to 0100/NEXT.LOG ***",
            "error": "line does not match expected LOG pattern",
        },
        {
            "run_id": "run-2",
            "instrument_id": "0100",
            "source_file": log_path.as_posix(),
            "line_number": 10,
            "raw_line": "1700000006:Command list",
            "error": "line does not match expected LOG pattern",
        },
    ]


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
