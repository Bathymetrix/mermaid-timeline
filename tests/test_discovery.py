# SPDX-License-Identifier: MIT

from pathlib import Path

import pytest

from mermaid_records.discovery import (
    iter_bin_files,
    iter_log_files,
    iter_mer_files,
    iter_raw_inputs,
)


def test_iter_bin_files_recurses(tmp_path: Path) -> None:
    _build_tree(tmp_path)

    paths = list(iter_bin_files(tmp_path))

    assert {path.name for path in paths} == {"0088_68E2D462.BIN", "0100_685864F3.BIN"}


def test_iter_mer_files_recurses(tmp_path: Path) -> None:
    _build_tree(tmp_path)

    paths = list(iter_mer_files(tmp_path))

    assert {path.name for path in paths} == {"0088_68E2D462.MER", "0100_685864F3.MER"}


def test_iter_log_files_recurses(tmp_path: Path) -> None:
    _build_tree(tmp_path)

    paths = list(iter_log_files(tmp_path))

    assert {path.name for path in paths} == {"0088_68E2D462.LOG", "0100_685864F3.LOG"}


def test_iter_raw_inputs_combined(tmp_path: Path) -> None:
    _build_tree(tmp_path)

    paths = list(iter_raw_inputs(tmp_path))

    assert {path.name for path in paths} == {
        "0088_68E2D462.BIN",
        "0100_685864F3.BIN",
        "0088_68E2D462.LOG",
        "0100_685864F3.LOG",
        "0088_68E2D462.MER",
        "0100_685864F3.MER",
    }


def test_iter_raw_inputs_kinds_filter(tmp_path: Path) -> None:
    _build_tree(tmp_path)

    paths = list(iter_raw_inputs(tmp_path, kinds=("mer",)))

    assert {path.suffix for path in paths} == {".MER"}


def test_iter_raw_inputs_sorted(tmp_path: Path) -> None:
    _build_tree(tmp_path)

    paths = list(iter_raw_inputs(tmp_path, sort=True))

    assert paths == sorted(paths)


def test_iter_raw_inputs_ignores_non_matching_files(tmp_path: Path) -> None:
    _build_tree(tmp_path)

    paths = list(iter_raw_inputs(tmp_path))

    assert {path.name for path in paths}.isdisjoint(
        {
            "0088_68E2D462.C.csv",
            "0088_68E2D462.C.html",
            "0088_68E2D462.S41",
            "0088_68E2D462.S61",
            "0088_68E2D462.RBR",
        }
    )


def test_iter_raw_inputs_requires_existing_directory(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        list(iter_raw_inputs(tmp_path / "missing"))

    not_a_directory = tmp_path / "single.MER"
    not_a_directory.write_text("", encoding="utf-8")

    with pytest.raises(NotADirectoryError):
        list(iter_raw_inputs(not_a_directory))


def _build_tree(root: Path) -> None:
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s").mkdir(parents=True)
    (root / "other" / "nested").mkdir(parents=True)

    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.MER").write_bytes(
        b""
    )
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.LOG").write_text(
        "",
        encoding="utf-8",
    )
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.BIN").write_bytes(
        b""
    )
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.C.csv").write_text(
        "",
        encoding="utf-8",
    )
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.C.html").write_text(
        "",
        encoding="utf-8",
    )
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.S41").write_bytes(
        b""
    )
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.S61").write_bytes(
        b""
    )
    (root / "467.164-T-0102" / "0088_20251005-20h26m10s" / "0088_68E2D462.RBR").write_bytes(
        b""
    )
    (root / "other" / "nested" / "0100_685864F3.MER").write_bytes(b"")
    (root / "other" / "nested" / "0100_685864F3.LOG").write_text("", encoding="utf-8")
    (root / "other" / "nested" / "0100_685864F3.BIN").write_bytes(b"")
