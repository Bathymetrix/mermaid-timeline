"""Parser interfaces for MERMAID .CYCLE.h text files."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from .models import LogEvent, LogEventType


def iter_cycle_events(path: Path) -> Iterator[LogEvent]:
    """Yield conservative events from a .CYCLE.h text file."""

    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue
            yield LogEvent(
                line_number=line_number,
                event_type=_infer_event_type(line),
                message=line,
                raw_line=line,
            )


def _infer_event_type(line: str) -> LogEventType:
    """Infer a basic event type from raw cycle text."""

    lowered = line.lower()
    if "error" in lowered:
        return LogEventType.ERROR
    if "warn" in lowered:
        return LogEventType.WARNING
    if "debug" in lowered:
        return LogEventType.DEBUG
    if "info" in lowered:
        return LogEventType.INFO
    return LogEventType.UNKNOWN
