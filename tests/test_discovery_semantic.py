# SPDX-License-Identifier: MIT

from pathlib import Path

from mermaid_timeline.discovery import (
    iter_emitted_cycle_files,
    iter_processed_cycle,
    iter_processed_cycle_h_files,
    iter_processed_mer_env_files,
    iter_server_mer,
)


def test_iter_server_mer_is_mer_alias(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "0001_ABCDEF01.MER").write_bytes(b"")

    paths = list(iter_server_mer(tmp_path))

    assert [path.name for path in paths] == ["0001_ABCDEF01.MER"]


def test_iter_processed_cycle_is_cycle_alias(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "0001_ABCDEF01.CYCLE.h").write_text("", encoding="utf-8")

    paths = list(iter_processed_cycle(tmp_path))

    assert [path.name for path in paths] == ["0001_ABCDEF01.CYCLE.h"]


def test_iter_processed_cycle_h_files_is_explicit_alias(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "0001_ABCDEF01.CYCLE.h").write_text("", encoding="utf-8")

    paths = list(iter_processed_cycle_h_files(tmp_path))

    assert [path.name for path in paths] == ["0001_ABCDEF01.CYCLE.h"]


def test_iter_emitted_cycle_files_discovers_raw_cycle(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "0001_ABCDEF01.CYCLE").write_text("", encoding="utf-8")
    (tmp_path / "nested" / "0001_ABCDEF01.CYCLE.h").write_text("", encoding="utf-8")

    paths = list(iter_emitted_cycle_files(tmp_path))

    assert [path.name for path in paths] == ["0001_ABCDEF01.CYCLE"]


def test_iter_processed_mer_env_files_is_explicit_alias(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "0001_ABCDEF01.LOG.0001_ABCDEF01.MER.env").write_text(
        "",
        encoding="utf-8",
    )

    paths = list(iter_processed_mer_env_files(tmp_path))

    assert [path.name for path in paths] == ["0001_ABCDEF01.LOG.0001_ABCDEF01.MER.env"]
