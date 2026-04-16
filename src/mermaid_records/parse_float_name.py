# SPDX-License-Identifier: MIT

"""Canonical parsing helpers for MERMAID instrument serial names."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


_SERIAL_RE = re.compile(
    r"^(?P<kinst>\d+\.\d+)-(?P<float_code>[A-Z]+)-(?P<float_number>\d+)$"
)


@dataclass(frozen=True, slots=True)
class FloatName:
    """Canonical decomposition of an Osean instrument serial name."""

    serial: str
    kinst: str
    float_code: str
    float_number: str
    instrument_id: str
    kstnm: str
    raw_file_prefix: str


def parse_float_name(serial: str) -> FloatName:
    """Parse one canonical instrument serial name."""

    match = _SERIAL_RE.fullmatch(serial)
    if match is None:
        raise ValueError(f"Unsupported instrument serial name: {serial}")

    kinst = match.group("kinst")
    float_code = match.group("float_code")
    float_number = match.group("float_number")
    padded_number = float_number.zfill(5 - len(float_code))
    if len(float_code + padded_number) > 5:
        raise ValueError(f"Instrument code and number exceed 5-char station limit: {serial}")

    return FloatName(
        serial=serial,
        kinst=kinst,
        float_code=float_code,
        float_number=float_number,
        instrument_id=f"{float_code}{padded_number}",
        kstnm=f"{float_code}{padded_number}",
        raw_file_prefix=float_number,
    )


def maybe_parse_float_name(serial: str) -> FloatName | None:
    """Parse one instrument serial name when possible."""

    try:
        return parse_float_name(serial)
    except ValueError:
        return None


def float_name_from_vit_path(path: Path) -> FloatName | None:
    """Parse an instrument name from one .vit path when possible."""

    return maybe_parse_float_name(path.stem)
