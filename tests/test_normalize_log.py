# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path

from mermaid_records.normalize_log import write_log_jsonl_prototypes

FIXTURES_ROOT = (
    Path(__file__).resolve().parents[1] / "data" / "fixtures" / "467.174-T-0100" / "log"
)


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
    assert summary.testmode_records == 0
    assert summary.sbe_records == 0
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
    testmode_records = _read_jsonl(output_dir / "log_testmode_records.jsonl")
    sbe_records = _read_jsonl(output_dir / "log_sbe_records.jsonl")
    transmission_records = _read_jsonl(output_dir / "log_transmission_records.jsonl")
    measurement_records = _read_jsonl(output_dir / "log_measurement_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert len(operational_records) == 6
    assert acquisition_records == []
    assert ascent_request_records == []
    assert gps_records == []
    assert parameter_records == []
    assert testmode_records == []
    assert sbe_records == []
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
    assert operational_records[0]["source_file"] == log_path.name
    assert "time" not in operational_records[0]

    assert transmission_records[1]["referenced_artifact"] == "0100_AAAA0001.MER"
    assert transmission_records[1]["rate_bytes_per_s"] == 83
    assert transmission_records[1]["record_time"] == "2023-11-14T22:13:21"
    assert transmission_records[1]["log_epoch_time"] == "1700000001"
    assert transmission_records[1]["source_file"] == log_path.name
    assert "time" not in transmission_records[1]

    assert measurement_records[0]["measurement_kind"] == "pressure_temperature"
    assert measurement_records[1]["measurement_kind"] == "pump_duration"
    assert measurement_records[0]["record_time"] == "2023-11-14T22:13:22"
    assert measurement_records[0]["log_epoch_time"] == "1700000002"
    assert measurement_records[0]["source_file"] == log_path.name
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
    assert summary.testmode_records == 0
    assert summary.sbe_records == 0
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
    assert summary.testmode_records == 0
    assert summary.sbe_records == 0
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
    assert summary.testmode_records == 0
    assert summary.sbe_records == 0
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
        "source_file": log_path.name,
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

    assert summary.total_records == 7
    assert summary.operational_records == 4
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
        "*** switching to 0100/NEXT.LOG ***",
        "buoy 467.174-T-0100",
    ]
    rollover_record = operational_records[2]
    assert rollover_record["switched_to_log_file"] == "0100_NEXT.LOG"
    assert rollover_record["source_file"] == log_path.name
    assert malformed_log_lines == [
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


def test_write_log_jsonl_prototypes_groups_testmode_fixture_session_from_0100_examples(
    tmp_path: Path,
) -> None:
    log_path = FIXTURES_ROOT / "0100_64511916.LOG"
    output_dir = tmp_path / "jsonl"
    malformed_log_lines: list[dict[str, object]] = []

    summary = write_log_jsonl_prototypes(
        [log_path],
        output_dir,
        run_id="run-testmode",
        malformed_log_lines=malformed_log_lines,
    )

    testmode_records = _read_jsonl(output_dir / "log_testmode_records.jsonl")
    sbe_records = _read_jsonl(output_dir / "log_sbe_records.jsonl")

    assert summary.testmode_records == 1
    assert len(testmode_records) == 1
    assert summary.sbe_records >= 1
    assert testmode_records[0]["instrument_id"] == "0100"
    assert testmode_records[0]["source_file"] == log_path.name
    assert testmode_records[0]["episode_index"] == 0
    assert testmode_records[0]["start_log_epoch_time"] == "1683036460"
    assert testmode_records[0]["end_log_epoch_time"] == "1683036824"
    assert testmode_records[0]["raw_lines"][0] == "1683036460:[TESTMD,0053]Enter in test mode? yes/no"
    assert "Command list for MOBY 4000m" in testmode_records[0]["raw_lines"]
    assert "Set params" in testmode_records[0]["raw_lines"]
    assert "1683036482:[SURF  ,0025]Iridium..." in testmode_records[0]["raw_lines"]
    assert testmode_records[0]["raw_lines"][-1] == "1683036824:[TESTMD,0252]0100>"
    assert all("Command list" not in row["raw_line"] for row in malformed_log_lines)
    assert all("Iridium..." not in row["raw_line"] for row in malformed_log_lines)
    assert sbe_records[0]["raw_lines"][0].startswith("1683036452:[SBE   ,0391]Mode changed")


def test_write_log_jsonl_prototypes_groups_sbe_and_profil_fixture_blocks_from_0100_examples(
    tmp_path: Path,
) -> None:
    log_path = FIXTURES_ROOT / "0100_6491453E.LOG"
    output_dir = tmp_path / "jsonl"
    malformed_log_lines: list[dict[str, object]] = []

    summary = write_log_jsonl_prototypes(
        [log_path],
        output_dir,
        run_id="run-sbe",
        malformed_log_lines=malformed_log_lines,
    )

    sbe_records = _read_jsonl(output_dir / "log_sbe_records.jsonl")
    parameter_records = _read_jsonl(output_dir / "log_parameter_records.jsonl")
    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")

    assert summary.sbe_records == 6
    assert len(sbe_records) == 6
    assert summary.parameter_records == 0
    assert parameter_records == []
    assert sbe_records[0]["instrument_id"] == "0100"
    assert sbe_records[0]["source_file"] == log_path.name
    assert sbe_records[0]["episode_index"] == 0
    assert sbe_records[0]["start_log_epoch_time"] == "1687246390"
    assert sbe_records[0]["end_log_epoch_time"] == "1687246390"
    assert sbe_records[0]["raw_lines"][0] == "1687246390:[STAGE ,0091]Stage [1] surfacing 43200s (<93600s) SBE61 "
    assert sbe_records[0]["raw_lines"][-1] == "1687246390:[PROFIL,0299]    speed_control=10mbar/s"
    assert any("[PROFIL,0284]" in line for line in sbe_records[0]["raw_lines"])
    assert "turn off bluetooth" in {record["message"] for record in operational_records}
    assert all("manual_profil=1" not in row["raw_line"] for row in malformed_log_lines)


def test_write_log_jsonl_prototypes_groups_contiguous_sbe61_measurements_from_0100_examples(
    tmp_path: Path,
) -> None:
    log_path = FIXTURES_ROOT / "0100_649FF25E.LOG"
    output_dir = tmp_path / "jsonl"
    malformed_log_lines: list[dict[str, object]] = []

    summary = write_log_jsonl_prototypes(
        [log_path],
        output_dir,
        run_id="run-sbe61",
        malformed_log_lines=malformed_log_lines,
    )

    sbe_records = _read_jsonl(output_dir / "log_sbe_records.jsonl")

    assert summary.sbe_records >= 3
    measurement_episode = next(
        record
        for record in sbe_records
        if record["raw_lines"][0] == "1688233527:[SBE   ,0391]Mode changed from UPDATE to START"
    )
    assert measurement_episode["raw_lines"][1] == "1688233527:[SBE   ,0385]Start manual acquisitions"
    assert measurement_episode["raw_lines"][2] == "1688233527:[SBE   ,0391]Mode changed from START to PROFILING"
    assert measurement_episode["raw_lines"][3] == "1688233561:[SBE61 ,0396]P +20122,T +19514,S +34584"
    assert measurement_episode["line_end_index"] == measurement_episode["line_start_index"] + 3
    assert measurement_episode["end_log_epoch_time"] == "1688233561"
    assert all("[SBE61 ,0396]" not in row["raw_line"] for row in malformed_log_lines)


def test_write_log_jsonl_prototypes_broadens_transmission_classification_conservatively(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0700_transmission.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:[UPLOAD,0248]Upload data files...",
                '1700000001:[UPLOAD,0231]"0070/60B742A0.MER" uploaded at 137bytes/s',
                "1700000002:[MRMAID,0604]1373 bytes in 0026/5D3CDEA0.MER",
                "1700000003:[ZTX   ,486]peer ask to resume 07/5B6A9B02.LOG from byte 1024",
                "1700000004:[ZTX   ,472]<ERR>peer ask to resume 0048/607503A2.MER (118847bytes) from byte 4294967294",
                '1700000005:[UPLOAD,9999]<ERR>upload "0026","0026/5DC0FCFC.MER"',
                "1700000006:[SURF  ,0069]transfer interrupted , retry",
                "1700000007:[MAIN  ,0013]2 file(s) uploaded",
                "1700000008:[SURF  ,0014]disconnected after 288s",
                "1700000009:[SURF  ,0025]Iridium...",
                "1700000010:[SURF  ,0226]Go dive (Minimum surface delay expired and no more file to upload)",
                "1700000011:[SURF  ,0056]<WARN>peer mute",
                "1700000012:[SURF  ,0071]<WARN>timeout",
                "1700000013:[SURF  ,0023]failed to connect #1, code -8, net 1, qual 5, dial 1",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    summary = write_log_jsonl_prototypes([log_path], output_dir)

    transmission_records = _read_jsonl(output_dir / "log_transmission_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert summary.transmission_records == 9
    assert [record["transmission_kind"] for record in transmission_records] == [
        "upload_batch",
        "upload_artifact",
        "upload_progress_artifact",
        "upload_resume",
        "upload_resume",
        "upload_error_artifact",
        "upload_retry",
        "upload_session_summary",
        "upload_disconnect",
    ]
    assert transmission_records[0]["transmission_kind"] == "upload_batch"
    assert transmission_records[0]["referenced_artifact"] is None
    assert transmission_records[0]["byte_count"] is None

    assert transmission_records[1]["transmission_kind"] == "upload_artifact"
    assert transmission_records[1]["referenced_artifact"] == "0070_60B742A0.MER"
    assert transmission_records[1]["rate_bytes_per_s"] == 137

    assert transmission_records[2]["transmission_kind"] == "upload_progress_artifact"
    assert transmission_records[2]["referenced_artifact"] == "0026_5D3CDEA0.MER"
    assert transmission_records[2]["byte_count"] == 1373
    assert transmission_records[2]["rate_bytes_per_s"] is None

    assert transmission_records[3]["transmission_kind"] == "upload_resume"
    assert transmission_records[3]["referenced_artifact"] == "07_5B6A9B02.LOG"
    assert transmission_records[3]["byte_offset"] == 1024
    assert transmission_records[3]["artifact_size_bytes"] is None

    assert transmission_records[4]["transmission_kind"] == "upload_resume"
    assert transmission_records[4]["referenced_artifact"] == "0048_607503A2.MER"
    assert transmission_records[4]["byte_offset"] == 4294967294
    assert transmission_records[4]["artifact_size_bytes"] == 118847
    assert transmission_records[4]["message"].startswith("<ERR>peer ask to resume")

    assert transmission_records[5]["transmission_kind"] == "upload_error_artifact"
    assert transmission_records[5]["referenced_artifact"] == "0026_5DC0FCFC.MER"

    assert transmission_records[6]["transmission_kind"] == "upload_retry"
    assert transmission_records[6]["referenced_artifact"] is None

    assert transmission_records[7]["transmission_kind"] == "upload_session_summary"
    assert transmission_records[7]["uploaded_file_count"] == 2

    assert transmission_records[8]["transmission_kind"] == "upload_disconnect"
    assert transmission_records[8]["disconnect_duration_s"] == 288

    assert {record["message"] for record in unclassified_records} >= {
        "Iridium...",
        "Go dive (Minimum surface delay expired and no more file to upload)",
        "<WARN>peer mute",
        "<WARN>timeout",
        "failed to connect #1, code -8, net 1, qual 5, dial 1",
    }


def test_write_log_jsonl_prototypes_classifies_wrapped_tagged_transmission_lines(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0700_wrapped.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:<ERR>[ZTX   ,472]peer ask to resume 0048/607503A2.MER (118847bytes) from byte 4294967294",
                "1700000001:<WRN>[SURF  ,0069]transfer interrupted , retry",
                "",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "jsonl"
    summary = write_log_jsonl_prototypes([log_path], output_dir)

    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")
    transmission_records = _read_jsonl(output_dir / "log_transmission_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert summary.operational_records == 2
    assert summary.transmission_records == 2
    assert summary.unclassified_records == 0
    assert [record["transmission_kind"] for record in transmission_records] == [
        "upload_resume",
        "upload_retry",
    ]
    assert transmission_records[0]["referenced_artifact"] == "0048_607503A2.MER"
    assert transmission_records[0]["artifact_size_bytes"] == 118847
    assert transmission_records[0]["byte_offset"] == 4294967294
    assert operational_records[0]["severity"] == "err"
    assert operational_records[1]["severity"] == "warn"
    assert operational_records[1]["message"] == "<WRN>transfer interrupted , retry"
    assert unclassified_records == []


def test_write_log_jsonl_prototypes_routes_wrapped_nonfamily_lines_to_unclassified(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0700_wrapped_unclassified.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:<ERR>[MODEM ,0347]ping error",
                "1700000001:<WRN>[SURF  ,0056]peer mute",
                "1700000002:<WARN>[MAIN  ,0041]mission empty",
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
        run_id="run-wrapped-unclassified",
        malformed_log_lines=malformed_log_lines,
    )

    operational_records = _read_jsonl(output_dir / "log_operational_records.jsonl")
    unclassified_records = _read_jsonl(output_dir / "log_unclassified_records.jsonl")

    assert summary.operational_records == 3
    assert summary.unclassified_records == 3
    assert summary.transmission_records == 0
    assert malformed_log_lines == []
    assert [record["message"] for record in unclassified_records] == [
        "<ERR>ping error",
        "<WRN>peer mute",
        "<WARN>mission empty",
    ]
    assert [record["severity"] for record in operational_records] == [
        "err",
        "warn",
        "warn",
    ]
    assert all(record["unclassified_reason"] == "no_family_match" for record in unclassified_records)


def test_write_log_jsonl_prototypes_keeps_true_unparsable_junk_malformed(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "0700_bad.LOG"
    log_path.write_text(
        "\n".join(
            [
                "1700000000:<ERR>broken wrapper without subsystem tag",
                "not even timestamped",
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
        run_id="run-malformed",
        malformed_log_lines=malformed_log_lines,
    )

    assert summary.operational_records == 0
    assert summary.unclassified_records == 0
    assert [row["raw_line"] for row in malformed_log_lines] == [
        "1700000000:<ERR>broken wrapper without subsystem tag",
        "not even timestamped",
    ]


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
