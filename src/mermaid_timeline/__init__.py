"""Top-level package for mermaid_timeline."""

from .cycle_raw import iter_cycle_events
from .models import (
    AcquisitionWindow,
    LogEvent,
    LogEventType,
    MerRecord,
    ProductCoverage,
    TimelineStatus,
    TimelineStatusKind,
)

__all__ = [
    "AcquisitionWindow",
    "LogEvent",
    "LogEventType",
    "MerRecord",
    "ProductCoverage",
    "TimelineStatus",
    "TimelineStatusKind",
    "iter_cycle_events",
]

__version__ = "0.1.0"
