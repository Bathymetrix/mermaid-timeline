# SPDX-License-Identifier: MIT

"""Detected product coverage helpers."""

from __future__ import annotations

from collections.abc import Iterable

from .models import ProductCoverage


def collect_detected_coverage(
    coverage: Iterable[ProductCoverage],
) -> list[ProductCoverage]:
    """Return detected coverage items as a concrete list."""

    return list(coverage)
