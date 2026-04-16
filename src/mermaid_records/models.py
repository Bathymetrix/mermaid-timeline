# SPDX-License-Identifier: MIT

"""Core typed models used across the normalization package."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

type OperationalSourceKind = Literal["log"]


@dataclass(slots=True)
class OperationalLogEntry:
    """One parsed operational LOG line of the form timestamp:[TAG]message.

    Represents a successfully parsed tagged LOG entry. Does not include
    continuation lines, console output, or other non-tagged LOG content.
    """

    time: datetime
    subsystem: str
    code: str | None
    message: str
    source_kind: OperationalSourceKind
    raw_line: str
    source_file: Path


@dataclass(slots=True)
class MerFileMetadata:
    """File-level metadata extracted conservatively from <ENVIRONMENT> through </PARAMETERS> in a .MER file.

    Includes raw lines and minimally parsed fields without interpretation.
    """

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
class MerEventBlock:
    """Metadata and payload for one <EVENT> block in a .MER file.

    Covers parsed fields from <INFO> and <FORMAT>, and the raw binary payload
    from <DATA> (excluding framing delimiters). No interpretation of the
    payload is performed.
    """

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
