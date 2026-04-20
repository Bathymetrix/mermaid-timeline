# SPDX-License-Identifier: MIT

from __future__ import annotations

from pathlib import Path
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[1]


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


def test_readme_documents_release_cli_contract() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "mermaid-records normalize" in readme
    assert "--input-root" in readme
    assert "--input-file" in readme
    assert "--dry-run" in readme
    assert "stateful mode" in readme
    assert "stateless mode" in readme
    assert "preflight_status.json" in readme


def test_root_license_file_is_present() -> None:
    license_text = (REPO_ROOT / "LICENSE").read_text(encoding="utf-8")

    assert license_text.startswith("MIT License")


def test_package_root_exposes_only_conservative_metadata_surface() -> None:
    import mermaid_records

    assert mermaid_records.__all__ == [
        "__version__",
        "__author__",
        "__license__",
        "__copyright__",
    ]
    assert hasattr(mermaid_records, "__version__")
    assert not hasattr(mermaid_records, "write_log_jsonl_prototypes")
    assert not hasattr(mermaid_records, "write_mer_jsonl_prototypes")
