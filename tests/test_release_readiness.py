# SPDX-License-Identifier: MIT

from __future__ import annotations

import contextlib
import importlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
from zipfile import ZipFile

from packaging.version import Version
import pytest

from mermaid_records import __version__
from mermaid_records.bin2log import Bin2LogConfig
from mermaid_records.normalize_pipeline import run_normalization_pipeline

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_VERSION = Version(__version__)
NORMALIZED_SOURCE_VERSION = str(SOURCE_VERSION)
FINAL_RELEASE_ENV_VAR = "MERMAID_RECORDS_REQUIRE_FINAL_RELEASE"
EXPECTED_FINAL_RELEASE_VERSION = Version("1.0.0")
EXPECTED_MIN_PYTHON = "3.12"


def test_pyproject_uses_dynamic_version_and_release_description() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["project"]["name"] == "mermaid-records"
    assert pyproject["project"]["dynamic"] == ["version"]
    assert pyproject["tool"]["setuptools"]["dynamic"]["version"] == {
        "attr": "mermaid_records.__version__"
    }
    assert pyproject["project"]["description"] == (
        "Canonical normalization of raw MERMAID BIN, LOG, and MER data into JSONL record families."
    )
    assert pyproject["project"]["requires-python"] == f">={EXPECTED_MIN_PYTHON}"
    assert pyproject["project"]["license"] == "MIT"
    assert "Programming Language :: Python :: 3.12" in pyproject["project"]["classifiers"]


def test_readme_documents_release_cli_contract_and_python_support() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert f"python-{EXPECTED_MIN_PYTHON}%2B" in readme
    assert "python-3.14%2B" not in readme
    assert "mermaid-records normalize" in readme
    assert "--input-root" in readme
    assert "--input-file" in readme
    assert "--dry-run" in readme
    assert "--json" in readme
    assert "--preflight-mode {strict,cached}" in readme
    assert "stateful mode" in readme
    assert "stateless mode" in readme
    assert "preflight_status.json" in readme
    assert "the field is absent rather than `null`" in readme
    assert "does not silently duplicate JSONL rows" in readme


def test_release_version_and_built_wheel_metadata_stay_in_sync(tmp_path: Path) -> None:
    wheel_path = _build_release_wheel(tmp_path)
    metadata = _wheel_metadata(wheel_path)

    assert wheel_path.name.startswith(f"mermaid_records-{NORMALIZED_SOURCE_VERSION}-")
    assert metadata["Name"] == "mermaid-records"
    assert Version(metadata["Version"]) == SOURCE_VERSION
    assert metadata["Requires-Python"] == f">={EXPECTED_MIN_PYTHON}"


def test_final_release_version_gate() -> None:
    if os.environ.get(FINAL_RELEASE_ENV_VAR) != "1":
        pytest.skip(
            f"set {FINAL_RELEASE_ENV_VAR}=1 to require the final 1.0.0 release version"
        )

    assert SOURCE_VERSION == EXPECTED_FINAL_RELEASE_VERSION
    assert not SOURCE_VERSION.is_prerelease


def test_cli_docs_capture_current_mode_and_flag_contract() -> None:
    cli_doc = (REPO_ROOT / "docs/cli.md").read_text(encoding="utf-8")

    assert "--json requires --dry-run" in cli_doc
    assert "--preflight-mode {strict,cached}" in cli_doc
    assert "manifests/" in cli_doc
    assert "state/" in cli_doc
    assert "preflight_status.json" in cli_doc
    assert "can therefore appear in either execution mode" in cli_doc
    assert "omits `preflight_status` rather than storing `null`" in cli_doc
    assert "safe to rerun because stateless mode rewrites the targeted output families" in cli_doc


def test_limitations_doc_matches_current_preservation_and_mode_rules() -> None:
    limitations = (REPO_ROOT / "docs/limitations.md").read_text(encoding="utf-8")

    assert "Stateful mode:" in limitations
    assert "Stateless mode:" in limitations
    assert "writes no `manifests/`" in limitations
    assert "writes no `state/`" in limitations
    assert "preflight_status.json" in limitations
    assert "the field is absent rather than `null`" in limitations
    assert "raw_format_line = null" in limitations
    assert "payload byte counts measure only the bytes inside `<DATA>...</DATA>`" in limitations
    assert "reruns do not silently duplicate rows" in limitations


def test_readme_lists_release_facing_fixture_families() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "- `452.020-P-06`" in readme
    assert "- `465.152-R-0001`" in readme
    assert "- `467.174-T-0100`" in readme


def test_root_license_file_is_present() -> None:
    license_text = (REPO_ROOT / "LICENSE").read_text(encoding="utf-8")

    assert license_text.startswith("MIT License")


def test_package_root_exposes_only_conservative_metadata_surface() -> None:
    import mermaid_records

    assert mermaid_records.__version__ == __version__
    assert mermaid_records.__all__ == [
        "__version__",
        "__author__",
        "__license__",
        "__copyright__",
    ]
    assert hasattr(mermaid_records, "__version__")
    assert not hasattr(mermaid_records, "write_log_jsonl_families")
    assert not hasattr(mermaid_records, "write_mer_jsonl_families")
    assert not hasattr(mermaid_records, "write_log_jsonl_families")
    assert not hasattr(mermaid_records, "write_mer_jsonl_families")


def test_release_pipeline_does_not_carry_bin_preflight_status_into_non_bin_rerun(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    (input_root / "467.174-T-0100.vit").write_text("", encoding="utf-8")
    bin_path = input_root / "0100_first.BIN"
    bin_path.write_bytes(b"raw-bin")

    decoder = _write_decoder(tmp_path / "decoder.py", "decoded")
    output_root = tmp_path / "output"

    run_normalization_pipeline(
        input_root,
        output_dir=output_root,
        config=Bin2LogConfig(
            python_executable=Path(sys.executable),
            decoder_script=decoder,
        ),
    )

    instrument_dir = output_root / "467.174-T-0100"
    first_latest = _read_json(instrument_dir / "manifests" / "latest.json")

    assert first_latest["preflight_status"] == (
        f"manifests/runs/{first_latest['run_id']}/preflight_status.json"
    )
    assert (instrument_dir / "preflight_status.json").exists()
    assert (instrument_dir / first_latest["preflight_status"]).exists()

    bin_path.unlink()
    _write_log(input_root / "0100_second.LOG", "second")

    run_normalization_pipeline(input_root, output_dir=output_root)

    second_latest = _read_json(instrument_dir / "manifests" / "latest.json")
    second_run_dir = instrument_dir / "manifests" / "runs" / second_latest["run_id"]

    assert not (instrument_dir / "preflight_status.json").exists()
    assert "preflight_status" not in second_latest
    assert not (second_run_dir / "preflight_status.json").exists()


def _build_release_wheel(tmp_path: Path) -> Path:
    source_root = tmp_path / "source"
    dist_dir = tmp_path / "dist"
    source_root.mkdir()
    shutil.copy2(REPO_ROOT / "pyproject.toml", source_root / "pyproject.toml")
    shutil.copy2(REPO_ROOT / "README.md", source_root / "README.md")
    shutil.copy2(REPO_ROOT / "LICENSE", source_root / "LICENSE")
    shutil.copytree(REPO_ROOT / "src", source_root / "src")
    dist_dir.mkdir()
    build_meta = _load_setuptools_build_meta()
    if build_meta is not None:
        with contextlib.chdir(source_root):
            wheel_name = build_meta.build_wheel(str(dist_dir))
        wheel_path = dist_dir / wheel_name
        assert wheel_path.exists()
        return wheel_path

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            str(source_root),
            "--no-deps",
            "--wheel-dir",
            str(dist_dir),
        ],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    wheels = sorted(dist_dir.glob("*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def _load_setuptools_build_meta() -> object | None:
    try:
        return importlib.import_module("setuptools.build_meta")
    except ModuleNotFoundError:
        return None


def _wheel_metadata(path: Path) -> dict[str, str]:
    with ZipFile(path) as archive:
        metadata_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata_text = archive.read(metadata_name).decode("utf-8")

    metadata: dict[str, str] = {}
    for line in metadata_text.splitlines():
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        metadata[key] = value
    return metadata


def _write_log(path: Path, message: str) -> None:
    path.write_text(f"1700000000:[MAIN  ,0007]{message}\n", encoding="utf-8")


def _write_decoder(path: Path, message: str) -> Path:
    path.write_text(
        f"""
from pathlib import Path

def database_update(_arg):
    print("Update Databases")

def concatenate_files(path):
    return [path]

def concatenate_rbr_files(path):
    return [path]

def decrypt_all(path):
    workdir = Path(path)
    log = workdir / "0100_first.LOG"
    log.write_text("1700000000:[MAIN  ,0007]{message}\\n", encoding="utf-8")
    return [path]
""",
        encoding="utf-8",
    )
    return path


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
