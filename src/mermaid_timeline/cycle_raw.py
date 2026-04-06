"""Parser interfaces for MERMAID .CYCLE.h text files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Iterator

from .models import CycleLogEntry

_CYCLE_LINE_RE = re.compile(
    r"^(?P<time>.+?):\[(?P<tag>[^\]]+)\](?P<message>.*)$"
)


def iter_cycle_events(path: Path) -> Iterator[CycleLogEntry]:
    """Yield parsed .CYCLE.h lines as conservative structured entries."""

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
            yield CycleLogEntry(
                time=datetime.fromisoformat(match.group("time")),
                subsystem=subsystem,
                code=code,
                message=match.group("message"),
                raw_line=line,
                source_file=path,
            )


def _parse_tag(tag: str) -> tuple[str, str | None]:
    """Split a bracket tag into subsystem and optional code."""

    if "," not in tag:
        return tag.strip(), None
    subsystem, code = tag.split(",", maxsplit=1)
    return subsystem.strip(), code.strip() or None
