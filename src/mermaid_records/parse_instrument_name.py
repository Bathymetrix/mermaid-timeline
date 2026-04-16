# SPDX-License-Identifier: MIT

"""Canonical parsing helpers for MERMAID instrument serial names."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


_SERIAL_RE = re.compile(
    r"^(?P<kinst>\d+\.\d+)-(?P<instrument_code>[A-Z]+)-(?P<instrument_number>\d+)$"
)


@dataclass(frozen=True, slots=True)
class InstrumentName:
    """Canonical decomposition of an Osean instrument serial name."""

    serial: str
    kinst: str
    instrument_code: str
    instrument_number: str
    instrument_id: str
    kstnm: str
    raw_file_prefix: str


def parse_instrument_name(serial: str) -> InstrumentName:
    """Parse one canonical instrument serial name."""

    match = _SERIAL_RE.fullmatch(serial)
    if match is None:
        raise ValueError(f"Unsupported instrument serial name: {serial}")

    kinst = match.group("kinst")
    instrument_code = match.group("instrument_code")
    instrument_number = match.group("instrument_number")
    padded_number = instrument_number.zfill(5 - len(instrument_code))
    if len(instrument_code + padded_number) > 5:
        raise ValueError(f"Instrument code and number exceed 5-char station limit: {serial}")

    return InstrumentName(
        serial=serial,
        kinst=kinst,
        instrument_code=instrument_code,
        instrument_number=instrument_number,
        instrument_id=f"{instrument_code}{padded_number}",
        kstnm=f"{instrument_code}{padded_number}",
        raw_file_prefix=instrument_number,
    )


def maybe_parse_instrument_name(serial: str) -> InstrumentName | None:
    """Parse one instrument serial name when possible."""

    try:
        return parse_instrument_name(serial)
    except ValueError:
        return None


def instrument_name_from_vit_path(path: Path) -> InstrumentName | None:
    """Parse an instrument name from one .vit path when possible."""

    return maybe_parse_instrument_name(path.stem)
