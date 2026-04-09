# SPDX-License-Identifier: MIT

"""Ledger-like aggregation entry points."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import (
    AcquisitionWindow,
    MerDataBlock,
    OperationalLogEntry,
    ProductCoverage,
    TimelineStatus,
)


@dataclass(slots=True)
class TimelineLedger:
    """Simple placeholder container for parsed and normalized artifacts."""

    operational_entries: list[OperationalLogEntry] = field(default_factory=list)
    mer_data_blocks: list[MerDataBlock] = field(default_factory=list)
    acquisition_windows: list[AcquisitionWindow] = field(default_factory=list)
    detected_coverage: list[ProductCoverage] = field(default_factory=list)
    requested_coverage: list[ProductCoverage] = field(default_factory=list)
    statuses: list[TimelineStatus] = field(default_factory=list)
