"""Shared interval schema helpers."""

from __future__ import annotations

from typing import Literal

from mermaid_timeline import __version__

SCHEMA_VERSION = "0.1.0"
PACKAGE_NAME = "mermaid-timeline"

type Boundary = Literal["closed", "open_unknown"]
type IntervalType = Literal["buf", "det", "req"]


def generated_by() -> dict[str, str]:
    return {"package": PACKAGE_NAME, "version": __version__}
