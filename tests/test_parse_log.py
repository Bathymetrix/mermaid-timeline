# SPDX-License-Identifier: MIT

from pathlib import Path

from mermaid_records.parse_log import iter_operational_log_entries


def test_iter_operational_log_entries_parses_log_fixture_line() -> None:
    path = Path("data/fixtures/467.174-T-0100/log/0100_6858665E.LOG")
    first_entry = next(iter_operational_log_entries(path))

    assert first_entry.time.isoformat() == "2025-06-22T20:23:58"
    assert first_entry.subsystem == "MAIN"
    assert first_entry.code == "0007"
    assert first_entry.message == "buoy 467.174-T-0100"
    assert first_entry.source_kind == "log"
    assert first_entry.source_file == path


def test_iter_operational_log_entries_replaces_invalid_utf8_bytes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "sample.LOG"
    path.write_bytes(b"1700000000:[MAIN  ,0007]bad\xffbyte\n")

    first_entry = next(iter_operational_log_entries(path))

    assert first_entry.message == "bad\ufffdbyte"
    assert first_entry.source_kind == "log"


def test_iter_operational_log_entries_rejects_non_log_files(tmp_path: Path) -> None:
    path = tmp_path / "sample.MER"
    path.write_text(
        "2025-06-22T20:23:58:[PREPROCESS]Create 0100_6858665E.LOG\n",
        encoding="utf-8",
    )

    try:
        next(iter_operational_log_entries(path))
    except ValueError as exc:
        assert "Unsupported operational log source" in str(exc)
    else:
        raise AssertionError("expected ValueError for non-LOG input")
