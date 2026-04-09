# SPDX-License-Identifier: MIT

"""Parser interfaces for operational LOG, CYCLE, and .CYCLE.h text files."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Iterator

from .models import OperationalLogEntry, OperationalSourceKind

_CYCLE_LINE_RE = re.compile(
    r"^(?P<time>.+?):\[(?P<tag>[^\]]+)\](?P<message>.*)$"
)


def iter_operational_log_entries(path: Path) -> Iterator[OperationalLogEntry]:
    """Yield parsed operational text lines as conservative structured entries."""

    source_kind = _detect_source_kind(path)

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\r\n")
            if not line.strip():
                continue
            match = _CYCLE_LINE_RE.match(line)
            if not match:
                continue
            tag = match.group("tag")
            subsystem, code = _parse_tag(tag)
            yield OperationalLogEntry(
                time=_parse_time_text(match.group("time")),
                subsystem=subsystem,
                code=code,
                message=match.group("message"),
                source_kind=source_kind,
                raw_line=line,
                source_file=path,
            )


def iter_cycle_events(path: Path) -> Iterator[OperationalLogEntry]:
    """Backward-compatible alias for iterating parsed operational text lines."""

    yield from iter_operational_log_entries(path)


def _parse_tag(tag: str) -> tuple[str, str | None]:
    """Split a bracket tag into subsystem and optional code."""

    if "," not in tag:
        return tag.strip(), None
    subsystem, code = tag.split(",", maxsplit=1)
    return subsystem.strip(), code.strip() or None


def _detect_source_kind(path: Path) -> OperationalSourceKind:
    """Infer the operational source kind from the file name."""

    name = path.name
    if name.endswith(".CYCLE.h"):
        return "cycle_h"
    if name.endswith(".CYCLE"):
        return "cycle"
    if name.endswith(".LOG"):
        return "log"
    raise ValueError(f"Unsupported operational log source: {path}")


def _parse_time_text(text: str) -> datetime:
    """Parse either an epoch-seconds prefix or an ISO timestamp."""

    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc).replace(tzinfo=None)
    return datetime.fromisoformat(text)
