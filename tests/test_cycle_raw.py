# SPDX-License-Identifier: MIT

from pathlib import Path

from mermaid_timeline.cycle_raw import iter_cycle_events, iter_operational_log_entries


def test_iter_operational_log_entries_parses_log_fixture_line() -> None:
    path = Path("data/fixtures/467.174-T-0100/log/0100_6858665E.LOG")
    first_entry = next(iter_operational_log_entries(path))

    assert first_entry.time.isoformat() == "2025-06-22T20:23:58"
    assert first_entry.subsystem == "MAIN"
    assert first_entry.code == "0007"
    assert first_entry.message == "buoy 467.174-T-0100"
    assert first_entry.source_kind == "log"
    assert first_entry.source_file == path


def test_iter_operational_log_entries_parses_cycle_fixture_line() -> None:
    path = Path("data/fixtures/467.174-T-0100/cycle/0075_6858665E.CYCLE")
    first_entry = next(iter_operational_log_entries(path))

    assert first_entry.time.isoformat() == "2025-06-22T20:23:58"
    assert first_entry.subsystem == "PREPROCESS"
    assert first_entry.code is None
    assert first_entry.message == "Create 0100_6858665E.LOG"
    assert first_entry.source_kind == "cycle"


def test_iter_operational_log_entries_parses_cycle_h_text(tmp_path: Path) -> None:
    path = tmp_path / "sample.CYCLE.h"
    path.write_text(
        "2025-06-22T20:23:58:[PREPROCESS]Create 0100_6858665E.LOG\n",
        encoding="utf-8",
    )
    first_entry = next(iter_operational_log_entries(path))

    assert first_entry.time.isoformat() == "2025-06-22T20:23:58"
    assert first_entry.source_kind == "cycle_h"
    assert first_entry.subsystem == "PREPROCESS"


def test_iter_cycle_events_remains_backward_compatible() -> None:
    path = Path("data/fixtures/467.174-T-0100/cycle/0075_6858665E.CYCLE")
    first_entry = next(iter_cycle_events(path))

    assert first_entry.source_kind == "cycle"
