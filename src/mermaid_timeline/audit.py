# SPDX-License-Identifier: MIT

"""Corpus audit helpers built on top of discovery and raw parsing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .cycle_raw import iter_cycle_events
from .discovery import iter_cycle_files, iter_mer_files
from .mer_raw import parse_mer_file


@dataclass(slots=True)
class MerCorpusStats:
    """Summary counts for a corpus of raw .MER files."""

    total_files: int
    parsed_ok: int
    empty_files: int
    non_empty_files: int


@dataclass(slots=True)
class CycleCorpusStats:
    """Summary counts for a corpus of processed .CYCLE.h files."""

    total_files: int
    parsed_ok: int
    parse_failures: int


def audit_server_mer(root: Path) -> MerCorpusStats:
    """Audit a server-style raw MER corpus rooted at ``root``."""

    total_files = 0
    parsed_ok = 0
    empty_files = 0
    non_empty_files = 0

    for path in iter_mer_files(root):
        total_files += 1
        _, blocks = parse_mer_file(path)
        parsed_ok += 1
        if len(blocks) == 0:
            empty_files += 1
        else:
            non_empty_files += 1

    return MerCorpusStats(
        total_files=total_files,
        parsed_ok=parsed_ok,
        empty_files=empty_files,
        non_empty_files=non_empty_files,
    )


def audit_processed_cycle(root: Path) -> CycleCorpusStats:
    """Audit a processed cycle corpus rooted at ``root``."""

    total_files = 0
    parsed_ok = 0
    parse_failures = 0

    for path in iter_cycle_files(root):
        total_files += 1
        try:
            for _ in iter_cycle_events(path):
                pass
        except Exception:
            parse_failures += 1
        else:
            parsed_ok += 1

    return CycleCorpusStats(
        total_files=total_files,
        parsed_ok=parsed_ok,
        parse_failures=parse_failures,
    )
