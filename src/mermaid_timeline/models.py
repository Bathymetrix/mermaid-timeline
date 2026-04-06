"""Core typed models used across the package."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class CycleLogEntry:
    """One parsed line from a .CYCLE.h text file."""

    time: datetime
    subsystem: str
    code: str | None
    message: str
    raw_line: str
    source_file: Path


@dataclass(slots=True)
class AcquisitionWindow:
    """Explicit acquisition start/stop pair from cycle text."""

    start: datetime
    stop: datetime
    source_file: Path


@dataclass(slots=True)
class MerFileMetadata:
    """File-level metadata extracted conservatively from a .MER file."""

    board: str | None
    software_version: str | None
    dive_id: int | None
    dive_event_count: int | None
    pool_event_count: int | None
    pool_size_bytes: int | None
    gps_fixes: list[dict[str, str]] = field(default_factory=list)
    drifts: list[dict[str, int | None]] = field(default_factory=list)
    clock_frequencies_hz: list[int] = field(default_factory=list)
    sample_min: int | None = None
    sample_max: int | None = None
    true_sample_freq_hz: float | None = None
    raw_environment_lines: list[str] = field(default_factory=list)
    raw_parameter_lines: list[str] = field(default_factory=list)
    source_file: Path | None = None


@dataclass(slots=True)
class MerDataBlock:
    """One transmitted data block from a .MER file."""

    date: datetime | None
    pressure_mbar: float | None
    temperature_c: float | None
    criterion: float | None
    snr: float | None
    trig: int | None
    detrig: int | None
    endianness: str | None
    bytes_per_sample: int | None
    sampling_rate_hz: float | None
    stages: int | None
    normalized: bool | None
    length_samples: int | None
    raw_info_line: str | None
    raw_format_line: str | None
    data_payload: bytes | None
    source_file: Path


@dataclass(slots=True)
class ProductCoverage:
    """Coverage summary for a derived or requested product."""

    product_name: str
    start: datetime | None = None
    end: datetime | None = None
    covered_fraction: float | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class TimelineStatus:
    """Status summary for a timeline segment or product."""

    kind: str = "unknown"
    detail: str = ""
    start: datetime | None = None
    end: datetime | None = None
    metadata: dict[str, object] = field(default_factory=dict)
