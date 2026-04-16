# SPDX-License-Identifier: MIT

from mermaid_records.parse_float_name import parse_float_name


def test_parse_float_name_zero_pads_station_suffix() -> None:
    parsed = parse_float_name("452.020-P-08")

    assert parsed.kinst == "452.020"
    assert parsed.serial == "452.020-P-08"
    assert parsed.instrument_id == "P0008"
    assert parsed.kstnm == "P0008"
    assert parsed.raw_file_prefix == "08"


def test_parse_float_name_preserves_existing_zero_padding() -> None:
    parsed = parse_float_name("467.174-T-0100")

    assert parsed.kinst == "467.174"
    assert parsed.instrument_id == "T0100"
    assert parsed.kstnm == "T0100"
    assert parsed.raw_file_prefix == "0100"
