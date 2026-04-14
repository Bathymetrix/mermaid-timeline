# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path

from mermaid_records.normalize_mer import write_mer_jsonl_prototypes


def test_write_mer_jsonl_prototypes_preserves_environment_parameter_and_data_rows(
    tmp_path: Path,
) -> None:
    mer_path = tmp_path / "0100_sample.MER"
    mer_path.write_bytes(
        (
            "<ENVIRONMENT>\n"
            "\t<BOARD 452116600-A0 />\n"
            "\t<SOFTWARE 2.1344 />\n"
            "\t<DIVE ID=2 EVENTS=9 />\n"
            "\t<POOL EVENTS=12 SIZE=275998 />\n"
            "\t<GPSINFO DATE=2024-02-07T22:47:22 LAT=+2845.7300 LON=+13848.3010 />\n"
            "\t<DRIFT SEC=-1 USEC=-108856 />\n"
            "\t<CLOCK Hz=3686332 />\n"
            "\t<SAMPLE MIN=-12392144 MAX=13882064 />\n"
            "\t<TRUE_SAMPLE_FREQ FS_Hz=40.014255 />\n"
            "</ENVIRONMENT>\n"
            "<PARAMETERS>\n"
            "\t<ADC GAIN=1 BUFFER=ON />\n"
            "\t<INPUT_FILTER CUTOFF=0.100000 POLES=4 />\n"
            "\t<STALTA STA=80 LTA=800 RATIO=2.100000 />\n"
            "\t<EVENT_LEN PRE=200 POST=4000 MIN=1024 MAX=8192 />\n"
            "\t<RATING MIN=0.100000 MAX=0.900000 />\n"
            "\t<CDF24 ENABLED=YES />\n"
            "\t<MODEL EXTRAPOLATED=0 REF0=0.0 WEIGHT0=1.0 />\n"
            "\t<ASCEND_THRESH PRESSURE=10 COUNTDOWN=3 />\n"
            "\t<MISC UPLOAD_MAX=100kB />\n"
            "</PARAMETERS>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-07T22:47:22 FNAME=2024-02-07T22_47_22.000000 "
            "SMP_OFFSET=614054 TRUE_FS=40.014107 />\n"
            "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            "STAGES=5 NORMALIZED=YES LENGTH=4832 />\n"
            "\t<DATA>ABC</DATA>\n"
            "</EVENT>\n"
            "<EVENT>\n"
            "\t<INFO DATE=2024-02-08T00:00:01.737670 PRESSURE=1504.00 TEMPERATURE=-11.0000 "
            "CRITERION=0.0296122 SNR=2.556 TRIG=2000 DETRIG=5819 />\n"
            "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            "STAGES=5 NORMALIZED=YES LENGTH=1024 />\n"
            "\t<DATA>WXYZ</DATA>\n"
            "</EVENT>\n"
        ).encode("ascii")
    )

    output_dir = tmp_path / "jsonl"
    summary = write_mer_jsonl_prototypes([mer_path], output_dir)

    environment_records = _read_jsonl(output_dir / "mer_environment_records.jsonl")
    parameter_records = _read_jsonl(output_dir / "mer_parameter_records.jsonl")
    data_records = _read_jsonl(output_dir / "mer_data_records.jsonl")

    assert summary.environment_records == 9
    assert summary.parameter_records == 9
    assert summary.data_records == 2
    assert summary.total_mer_files == 1
    assert summary.zero_event_files == 0
    assert summary.total_event_blocks == 2
    assert summary.environment_kind_counts == {
        "board": 1,
        "clock": 1,
        "dive": 1,
        "drift": 1,
        "gpsinfo": 1,
        "pool": 1,
        "sample": 1,
        "software": 1,
        "true_sample_freq": 1,
    }
    assert summary.parameter_kind_counts == {
        "adc": 1,
        "ascend_thresh": 1,
        "cdf24": 1,
        "event_len": 1,
        "input_filter": 1,
        "misc": 1,
        "model": 1,
        "rating": 1,
        "stalta": 1,
    }
    assert summary.unknown_environment_tags == []
    assert summary.unknown_parameter_tags == []
    assert summary.unknown_info_keys == []
    assert summary.unknown_format_keys == []

    gpsinfo_record = next(
        record for record in environment_records if record["environment_kind"] == "gpsinfo"
    )
    assert gpsinfo_record["gpsinfo_date"] == "2024-02-07T22:47:22"
    assert gpsinfo_record["raw_values"] == {
        "date": "2024-02-07T22:47:22",
        "lat": "+2845.7300",
        "lon": "+13848.3010",
    }
    assert gpsinfo_record["line"] == "\t<GPSINFO DATE=2024-02-07T22:47:22 LAT=+2845.7300 LON=+13848.3010 />"

    drift_record = next(
        record for record in environment_records if record["environment_kind"] == "drift"
    )
    assert drift_record["raw_values"] == {"sec": "-1", "usec": "-108856"}

    adc_record = next(
        record for record in parameter_records if record["parameter_kind"] == "adc"
    )
    assert adc_record["raw_values"] == {"buffer": "ON", "gain": "1"}

    model_record = next(
        record for record in parameter_records if record["parameter_kind"] == "model"
    )
    assert model_record["raw_values"] == {
        "extrapolated": "0",
        "ref0": "0.0",
        "weight0": "1.0",
    }

    assert data_records[0]["block_index"] == 0
    assert data_records[0]["date"] == "2024-02-07T22:47:22"
    assert data_records[0]["fname"] == "2024-02-07T22_47_22.000000"
    assert data_records[0]["smp_offset"] == "614054"
    assert data_records[0]["true_fs"] == "40.014107"
    assert data_records[0]["data_payload_nbytes"] == 3
    assert data_records[0]["raw_info_line"] == (
        '<INFO DATE=2024-02-07T22:47:22 FNAME=2024-02-07T22_47_22.000000 '
        'SMP_OFFSET=614054 TRUE_FS=40.014107 />'
    )

    assert data_records[1]["pressure"] == "1504.00"
    assert data_records[1]["temperature"] == "-11.0000"
    assert data_records[1]["criterion"] == "0.0296122"
    assert data_records[1]["snr"] == "2.556"
    assert data_records[1]["trig"] == "2000"
    assert data_records[1]["detrig"] == "5819"
    assert data_records[1]["length"] == "1024"
    assert data_records[1]["data_payload_nbytes"] == 4
    assert "record_time" not in data_records[1]
    assert "time" not in data_records[1]
    assert all(record["float_id"] == "0100" for record in data_records)


def test_write_mer_jsonl_prototypes_accepts_canonical_float_id_override(tmp_path: Path) -> None:
    mer_path = tmp_path / "0100_sample.MER"
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

    output_dir = tmp_path / "jsonl"
    write_mer_jsonl_prototypes([mer_path], output_dir, float_id="T0100")
    data_records = _read_jsonl(output_dir / "mer_data_records.jsonl")

    assert data_records[0]["float_id"] == "T0100"


def test_write_mer_jsonl_prototypes_counts_zero_event_files(tmp_path: Path) -> None:
    empty_mer = tmp_path / "0001_empty.MER"
    empty_mer.write_bytes(
        (
            "<ENVIRONMENT>\n"
            "\t<BOARD 452116600-A0 />\n"
            "</ENVIRONMENT>\n"
            "<PARAMETERS>\n"
            "\t<MISC UPLOAD_MAX=100kB />\n"
            "</PARAMETERS>\n"
        ).encode("ascii")
    )

    output_dir = tmp_path / "jsonl"
    summary = write_mer_jsonl_prototypes([empty_mer], output_dir)

    assert summary.total_mer_files == 1
    assert summary.zero_event_files == 1
    assert summary.total_event_blocks == 0
    assert _read_jsonl(output_dir / "mer_data_records.jsonl") == []


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
