"""JSONL record IO."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]
type JsonRows = Sequence[JsonObject]


@dataclass(frozen=True, slots=True)
class SourceRecord:
    line_number: int
    row: JsonObject


type SourceRecords = Sequence[SourceRecord]


def iter_jsonl(path: Path) -> Iterator[SourceRecord]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: JSONL row is not an object")
            yield SourceRecord(line_number=line_number, row=value)


def source_records(rows: Iterable[JsonObject]) -> list[SourceRecord]:
    return [
        SourceRecord(line_number=line_number, row=row)
        for line_number, row in enumerate(rows, start=1)
    ]


def write_jsonl(path: Path, rows: Iterable[JsonObject]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=False, separators=(",", ":")))
            handle.write("\n")
            count += 1
    return count
