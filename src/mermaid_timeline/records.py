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
    source_path: Path | None = None


type SourceRecords = Sequence[SourceRecord]


class InputFileError(ValueError):
    """Raised when an input artifact cannot be read or decoded."""


_MISSING = object()
_MAX_SNIPPET_LENGTH = 160


def iter_jsonl(path: Path) -> Iterator[SourceRecord]:
    try:
        handle = path.open("rb")
    except OSError as exc:
        raise InputFileError(
            format_input_error(
                "Unable to read JSONL input",
                file=path,
                expected="readable UTF-8 JSONL file",
                detail=str(exc),
            )
        ) from exc

    with handle:
        for line_number, raw_line in enumerate(handle, start=1):
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise InputFileError(
                    format_input_error(
                        "Invalid JSONL input encoding",
                        file=path,
                        line=line_number,
                        column=exc.start + 1,
                        expected="UTF-8 text",
                        detail=str(exc),
                    )
                ) from exc

            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise InputFileError(
                    format_input_error(
                        "Invalid JSONL record",
                        file=path,
                        line=line_number,
                        column=exc.colno,
                        snippet=line,
                        expected="one JSON object per line",
                        detail=exc.msg,
                    )
                ) from exc
            if not isinstance(value, dict):
                raise InputFileError(
                    format_input_error(
                        "Invalid JSONL record",
                        file=path,
                        line=line_number,
                        value=value,
                        expected="JSON object",
                    )
                )
            yield SourceRecord(line_number=line_number, row=value, source_path=path)


def display_path(path: Path | str) -> str:
    path_obj = Path(path)
    try:
        resolved = path_obj.resolve()
    except OSError:
        return str(path)
    try:
        return str(resolved.relative_to(Path.cwd().resolve()))
    except (OSError, ValueError):
        return str(resolved)


def format_input_error(
    title: str,
    *,
    file: Path | str,
    line: int | None = None,
    column: int | None = None,
    field: str | None = None,
    value: object = _MISSING,
    expected: str | None = None,
    snippet: str | None = None,
    detail: str | None = None,
) -> str:
    lines = [f"{title}:", f"  file: {display_path(file)}"]
    if line is not None:
        lines.append(f"  line: {line}")
    if column is not None:
        lines.append(f"  column: {column}")
    if field is not None:
        lines.append(f"  field: {field}")
    if value is not _MISSING:
        lines.append(f"  value: {_format_value(value)}")
    if expected is not None:
        lines.append(f"  expected: {expected}")
    if snippet is not None:
        lines.append(f"  snippet: {_snippet(snippet)}")
    if detail is not None:
        lines.append(f"  detail: {detail}")
    return "\n".join(lines)


def _format_value(value: object) -> str:
    try:
        text = json.dumps(value, ensure_ascii=True, sort_keys=True)
    except (TypeError, ValueError):
        text = repr(value)
    return _snippet(text)


def _snippet(text: str) -> str:
    collapsed = " ".join(text.strip().split())
    if len(collapsed) <= _MAX_SNIPPET_LENGTH:
        return collapsed
    return f"{collapsed[: _MAX_SNIPPET_LENGTH - 3]}..."


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
            handle.write(_json_dumps(row))
            handle.write("\n")
            count += 1
    return count


def _json_dumps(value: object, *, key: str | None = None) -> str:
    if isinstance(value, dict):
        return (
            "{"
            + ",".join(
                f"{json.dumps(item_key, ensure_ascii=True)}:"
                f"{_json_dumps(item_value, key=str(item_key))}"
                for item_key, item_value in value.items()
            )
            + "}"
        )
    if isinstance(value, list):
        return "[" + ",".join(_json_dumps(item) for item in value) + "]"
    if key == "duration" and isinstance(value, float):
        return format(value, ".6f")
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
