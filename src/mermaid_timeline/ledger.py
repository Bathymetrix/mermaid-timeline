# SPDX-License-Identifier: MIT

"""Ledger-like aggregation entry points."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import AcquisitionWindow, LogEvent, MerRecord, ProductCoverage, TimelineStatus


@dataclass(slots=True)
class TimelineLedger:
    """Simple container for parsed and derived timeline artifacts."""

    mer_records: list[MerRecord] = field(default_factory=list)
    log_events: list[LogEvent] = field(default_factory=list)
    acquisition_windows: list[AcquisitionWindow] = field(default_factory=list)
    detected_coverage: list[ProductCoverage] = field(default_factory=list)
    requested_coverage: list[ProductCoverage] = field(default_factory=list)
    statuses: list[TimelineStatus] = field(default_factory=list)
