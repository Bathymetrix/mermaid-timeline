# SPDX-License-Identifier: MIT

from pathlib import Path

import pytest

from mermaid_records.mer_raw import parse_mer_file


def test_parse_mer_file_extracts_metadata_and_blocks() -> None:
    path = Path("data/fixtures/467.174-T-0100/mer/0100_685864F3.MER")
    metadata, blocks = parse_mer_file(path)

    assert metadata.board == "452116600-A0"
    assert metadata.software_version == "2.1344"
    assert metadata.dive_id == 8
    assert metadata.dive_event_count == 41
    assert metadata.pool_event_count == 128
    assert metadata.pool_size_bytes == 2411800
    assert metadata.gps_fixes[0]["lat"] == "+3133.6840"
    assert metadata.clock_frequencies_hz[0] == 3686330
    assert metadata.sample_min == -134217728
    assert metadata.sample_max == 134217712
    assert metadata.true_sample_freq_hz == 40.014219
    assert len(blocks) >= 1
    assert blocks[0].date is not None
    assert blocks[0].length_samples == 4448
    assert blocks[0].endianness == "LITTLE"
    assert blocks[0].data_payload is not None


def test_parse_mer_file_extracts_only_payload_bytes_inside_data_framing(tmp_path: Path) -> None:
    path = tmp_path / "0100_framed.MER"
    payload = b"A" * 19328
    path.write_bytes(
        (
            b"<ENVIRONMENT>\n"
            b"\t<BOARD 452116600-A0 />\n"
            b"</ENVIRONMENT>\n"
            b"<PARAMETERS>\n"
            b"\t<MISC UPLOAD_MAX=100kB />\n"
            b"</PARAMETERS>\n"
            b"<EVENT>\n"
            b"\t<INFO DATE=2024-02-07T22:47:22 FNAME=framed.000000 SMP_OFFSET=614054 TRUE_FS=40.014107 />\n"
            b"\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            b"STAGES=5 NORMALIZED=YES LENGTH=4832 />\n"
            b"\t<DATA>\n\r"
            + payload
            + b"\n\r\t</DATA>\n"
            b"</EVENT>\n"
        )
    )

    _metadata, blocks = parse_mer_file(path)

    assert len(blocks) == 1
    assert blocks[0].data_payload == payload
    assert len(blocks[0].data_payload) == 19328


def test_parse_mer_file_rejects_incomplete_event_block(tmp_path: Path) -> None:
    path = tmp_path / "0100_incomplete.MER"
    path.write_bytes(
        (
            b"<ENVIRONMENT>\n"
            b"\t<BOARD 452116600-A0 />\n"
            b"</ENVIRONMENT>\n"
            b"<PARAMETERS>\n"
            b"\t<MISC UPLOAD_MAX=100kB />\n"
            b"</PARAMETERS>\n"
            b"<EVENT>\n"
            b"\t<INFO DATE=2024-02-07T22:47:22 FNAME=broken.000000 SMP_OFFSET=614054 TRUE_FS=40.014107 />\n"
            b"\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 "
            b"STAGES=5 NORMALIZED=YES LENGTH=4832 />\n"
            b"\t<DATA>\n\rABCDEF\n"
            b"</EVENT>\n"
        )
    )

    with pytest.raises(ValueError, match="missing </DATA>"):
        parse_mer_file(path)
