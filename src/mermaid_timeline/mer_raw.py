# SPDX-License-Identifier: MIT

"""Low-level interfaces for parsing raw MER files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Iterator

from .models import MerDataBlock, MerFileMetadata

_SECTION_RE = re.compile(
    rb"<(?P<name>ENVIRONMENT|PARAMETERS)>(?P<body>.*?)</(?P=name)>",
    re.DOTALL,
)
_EVENT_RE = re.compile(rb"<EVENT>(?P<body>.*?)</EVENT>", re.DOTALL)
_INFO_RE = re.compile(rb"<INFO (?P<body>[^>]+)/>")
_FORMAT_RE = re.compile(rb"<FORMAT (?P<body>[^>]+)/>")
_DATA_RE = re.compile(rb"<DATA>(?P<body>.*?)</DATA>", re.DOTALL)
_ATTR_RE = re.compile(r'([A-Za-z_]+)=([^\s>]+)')


def parse_mer_file(path: Path) -> tuple[MerFileMetadata, list[MerDataBlock]]:
    """Parse a .MER file into conservative file metadata and raw data blocks."""

    data = path.read_bytes()
    metadata = _parse_metadata(path, data)
    blocks = list(iter_mer_data_blocks(path, data))
    return metadata, blocks


def iter_mer_data_blocks(path: Path, data: bytes | None = None) -> Iterator[MerDataBlock]:
    """Yield conservative MER data blocks without interpreting waveform payloads."""

    raw_data = data if data is not None else path.read_bytes()
    for event_match in _EVENT_RE.finditer(raw_data):
        event_body = event_match.group("body")
        info_line = _extract_tag_line(event_body, _INFO_RE)
        format_line = _extract_tag_line(event_body, _FORMAT_RE)
        payload = _extract_payload(event_body)

        info_attrs = _parse_attributes(info_line)
        format_attrs = _parse_attributes(format_line)

        yield MerDataBlock(
            date=_parse_datetime(info_attrs.get("DATE")),
            pressure_mbar=_parse_float(info_attrs.get("PRESSURE")),
            temperature_c=_parse_float(info_attrs.get("TEMPERATURE")),
            criterion=_parse_float(info_attrs.get("CRITERION")),
            snr=_parse_float(info_attrs.get("SNR")),
            trig=_parse_int(info_attrs.get("TRIG")),
            detrig=_parse_int(info_attrs.get("DETRIG")),
            endianness=format_attrs.get("ENDIANNESS"),
            bytes_per_sample=_parse_int(format_attrs.get("BYTES_PER_SAMPLE")),
            sampling_rate_hz=_parse_float(format_attrs.get("SAMPLING_RATE")),
            stages=_parse_int(format_attrs.get("STAGES")),
            normalized=_parse_bool(format_attrs.get("NORMALIZED")),
            length_samples=_parse_int(format_attrs.get("LENGTH")),
            raw_info_line=info_line,
            raw_format_line=format_line,
            data_payload=payload,
            source_file=path,
        )


def iter_mer_records(path: Path) -> Iterator[MerDataBlock]:
    """Backward-compatible alias for iterating parsed MER data blocks."""

    yield from iter_mer_data_blocks(path)


def _parse_metadata(path: Path, data: bytes) -> MerFileMetadata:
    """Parse file-level metadata from ENVIRONMENT and PARAMETERS sections."""

    environment_bytes = _extract_section(data, b"ENVIRONMENT")
    parameter_bytes = _extract_section(data, b"PARAMETERS")

    environment_lines = _split_tag_lines(environment_bytes)
    parameter_lines = _split_tag_lines(parameter_bytes)

    metadata = MerFileMetadata(
        board=None,
        software_version=None,
        dive_id=None,
        dive_event_count=None,
        pool_event_count=None,
        pool_size_bytes=None,
        raw_environment_lines=environment_lines,
        raw_parameter_lines=parameter_lines,
        source_file=path,
    )

    for line in environment_lines:
        if line.startswith("<BOARD "):
            metadata.board = _parse_bare_tag_value(line, "BOARD")
        elif line.startswith("<SOFTWARE "):
            metadata.software_version = _parse_bare_tag_value(line, "SOFTWARE")
        elif line.startswith("<DIVE "):
            attrs = _parse_attributes(line)
            metadata.dive_id = _parse_int(attrs.get("ID"))
            metadata.dive_event_count = _parse_int(attrs.get("EVENTS"))
        elif line.startswith("<POOL "):
            attrs = _parse_attributes(line)
            metadata.pool_event_count = _parse_int(attrs.get("EVENTS"))
            metadata.pool_size_bytes = _parse_int(attrs.get("SIZE"))
        elif line.startswith("<GPSINFO "):
            attrs = _parse_attributes(line)
            metadata.gps_fixes.append(
                {
                    "date": attrs.get("DATE", ""),
                    "lat": attrs.get("LAT", ""),
                    "lon": attrs.get("LON", ""),
                }
            )
        elif line.startswith("<DRIFT "):
            attrs = _parse_attributes(line)
            metadata.drifts.append(
                {
                    "sec": _parse_int(attrs.get("SEC")),
                    "usec": _parse_int(attrs.get("USEC")),
                }
            )
        elif line.startswith("<CLOCK "):
            attrs = _parse_attributes(line)
            hz = _parse_int(attrs.get("Hz"))
            if hz is not None:
                metadata.clock_frequencies_hz.append(hz)
        elif line.startswith("<SAMPLE "):
            attrs = _parse_attributes(line)
            metadata.sample_min = _parse_int(attrs.get("MIN"))
            metadata.sample_max = _parse_int(attrs.get("MAX"))
        elif line.startswith("<TRUE_SAMPLE_FREQ "):
            attrs = _parse_attributes(line)
            metadata.true_sample_freq_hz = _parse_float(attrs.get("FS_Hz"))

    return metadata


def _extract_section(data: bytes, section_name: bytes) -> bytes:
    """Extract a named top-level section body."""

    for match in _SECTION_RE.finditer(data):
        if match.group("name") == section_name:
            return match.group("body")
    return b""


def _split_tag_lines(section: bytes) -> list[str]:
    """Split a decoded section into non-empty raw tag lines."""

    text = section.decode("ascii", "ignore")
    return [line.strip() for line in text.splitlines() if line.strip()]


def _extract_tag_line(event_body: bytes, pattern: re.Pattern[bytes]) -> str | None:
    """Extract a single INFO or FORMAT tag line from an event body."""

    match = pattern.search(event_body)
    if match is None:
        return None
    return match.group(0).decode("ascii", "ignore").strip()


def _extract_payload(event_body: bytes) -> bytes | None:
    """Extract the raw bytes inside a DATA tag."""

    match = _DATA_RE.search(event_body)
    if match is None:
        return None
    return match.group("body")


def _parse_attributes(line: str | None) -> dict[str, str]:
    """Parse XML-like KEY=value attributes from a line."""

    if line is None:
        return {}
    return {key: value for key, value in _ATTR_RE.findall(line)}


def _parse_bare_tag_value(line: str, tag_name: str) -> str | None:
    """Parse a single bare value from a tag like <BOARD value />."""

    prefix = f"<{tag_name} "
    suffix = " />"
    if line.startswith(prefix) and line.endswith(suffix):
        return line[len(prefix) : -len(suffix)]
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-like datetime string conservatively."""

    if value is None:
        return None
    return datetime.fromisoformat(value)


def _parse_int(value: str | None) -> int | None:
    """Parse an integer value conservatively."""

    if value is None:
        return None
    return int(value)


def _parse_float(value: str | None) -> float | None:
    """Parse a float value conservatively."""

    if value is None:
        return None
    return float(value)


def _parse_bool(value: str | None) -> bool | None:
    """Parse a simple YES/NO boolean value."""

    if value is None:
        return None
    if value == "YES":
        return True
    if value == "NO":
        return False
    return None
