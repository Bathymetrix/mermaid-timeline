from pathlib import Path

from mermaid_timeline.mer_raw import parse_mer_file


def test_parse_mer_file_extracts_metadata_and_blocks() -> None:
    path = Path("data/fixtures/0100_685864F3.MER")
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
