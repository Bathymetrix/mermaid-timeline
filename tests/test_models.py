# SPDX-License-Identifier: MIT

from datetime import datetime
from pathlib import Path

from mermaid_timeline.models import (
    AcquisitionWindow,
    CycleLogEntry,
    MerDataBlock,
    MerFileMetadata,
    ProductCoverage,
    TimelineStatus,
)


def test_dataclasses_can_be_instantiated() -> None:
    source = Path("fixture")
    entry = CycleLogEntry(
        time=datetime(2025, 1, 1, 0, 0, 0),
        subsystem="MRMAID",
        code="0002",
        message="acq started",
        raw_line="2025-01-01T00:00:00:[MRMAID,0002]acq started",
        source_file=source,
    )
    window = AcquisitionWindow(
        start=datetime(2025, 1, 1, 0, 0, 0),
        stop=datetime(2025, 1, 1, 0, 1, 0),
        source_file=source,
    )
    metadata = MerFileMetadata(
        board="452116600-A0",
        software_version="2.1344",
        dive_id=8,
        dive_event_count=41,
        pool_event_count=128,
        pool_size_bytes=2411800,
        source_file=source,
    )
    block = MerDataBlock(
        date=datetime(2025, 1, 1, 0, 0, 0),
        pressure_mbar=2000.0,
        temperature_c=33.0,
        criterion=0.5,
        snr=4.0,
        trig=2000,
        detrig=5000,
        endianness="LITTLE",
        bytes_per_sample=4,
        sampling_rate_hz=20.0,
        stages=5,
        normalized=True,
        length_samples=4448,
        raw_info_line="<INFO />",
        raw_format_line="<FORMAT />",
        data_payload=b"abc",
        source_file=source,
    )
    coverage = ProductCoverage(product_name="raw")
    status = TimelineStatus(kind="ok", detail="ready")

    assert entry.subsystem == "MRMAID"
    assert window.stop > window.start
    assert metadata.board == "452116600-A0"
    assert block.length_samples == 4448
    assert coverage.product_name == "raw"
    assert status.kind == "ok"
