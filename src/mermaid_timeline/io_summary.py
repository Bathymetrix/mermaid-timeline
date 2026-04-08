# SPDX-License-Identifier: MIT

"""Small I/O summary helpers."""

from __future__ import annotations

from pathlib import Path


def describe_path(path: Path) -> str:
    """Return a compact description for a file path."""

    return f"{path.name} ({path.suffix or 'no extension'})"
