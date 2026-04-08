# SPDX-License-Identifier: MIT

"""Timeline status helpers."""

from __future__ import annotations

from collections.abc import Iterable

from .models import TimelineStatus


def collect_timeline_statuses(
    statuses: Iterable[TimelineStatus],
) -> list[TimelineStatus]:
    """Return timeline statuses as a concrete list."""

    return list(statuses)
