"""Core typed models used across the package."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class LogEventType(StrEnum):
    """Conservative event categories for parsed cycle/log text events."""

    UNKNOWN = "unknown"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"


class TimelineStatusKind(StrEnum):
    """High-level timeline status categories."""

    UNKNOWN = "unknown"
    OK = "ok"
    PARTIAL = "partial"
    MISSING = "missing"
    ERROR = "error"


@dataclass(slots=True)
class MerRecord:
    """Low-level MER record with raw payload preserved."""

    offset: int
    record_type: str = "unknown"
    timestamp: datetime | None = None
    payload: bytes = b""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class LogEvent:
    """Conservative representation of a single LOG event."""

    line_number: int
    event_type: LogEventType = LogEventType.UNKNOWN
    timestamp: datetime | None = None
    message: str = ""
    raw_line: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AcquisitionWindow:
    """Time interval representing acquisition availability."""

    start: datetime | None = None
    end: datetime | None = None
    source: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ProductCoverage:
    """Coverage summary for a derived or requested product."""

    product_name: str
    start: datetime | None = None
    end: datetime | None = None
    covered_fraction: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TimelineStatus:
    """Status summary for a timeline segment or product."""

    kind: TimelineStatusKind = TimelineStatusKind.UNKNOWN
    detail: str = ""
    start: datetime | None = None
    end: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
