# SPDX-License-Identifier: MIT

"""Requested product coverage helpers."""

from __future__ import annotations

from collections.abc import Iterable

from .models import ProductCoverage


def collect_requested_coverage(
    coverage: Iterable[ProductCoverage],
) -> list[ProductCoverage]:
    """Return requested coverage items as a concrete list."""

    return list(coverage)
