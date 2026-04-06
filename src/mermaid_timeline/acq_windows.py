"""Acquisition window helpers."""

from __future__ import annotations

from collections.abc import Iterable

from .models import AcquisitionWindow, CycleLogEntry


def extract_acquisition_windows(
    entries: Iterable[CycleLogEntry],
) -> list[AcquisitionWindow]:
    """Extract explicit acquisition start/stop windows from cycle entries."""

    windows: list[AcquisitionWindow] = []
    open_start: CycleLogEntry | None = None

    for entry in entries:
        message = entry.message.strip().lower()
        if message == "acq started":
            if open_start is None:
                open_start = entry
            continue
        if message == "acq stopped" and open_start is not None:
            windows.append(
                AcquisitionWindow(
                    start=open_start.time,
                    stop=entry.time,
                    source_file=entry.source_file,
                )
            )
            open_start = None

    return windows
