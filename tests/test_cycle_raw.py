# SPDX-License-Identifier: MIT

from pathlib import Path

from mermaid_timeline.cycle_raw import iter_cycle_events


def test_iter_cycle_events_parses_fixture_line() -> None:
    path = Path("data/fixtures/0075_6858665E.CYCLE.h")
    first_entry = next(iter_cycle_events(path))

    assert first_entry.time.isoformat() == "2025-06-22T20:23:58"
    assert first_entry.subsystem == "PREPROCESS"
    assert first_entry.code is None
    assert first_entry.message == "Create 0100_6858665E.LOG"
    assert first_entry.source_file == path
