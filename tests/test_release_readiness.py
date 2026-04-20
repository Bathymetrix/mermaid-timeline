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
    assert "--json" in readme
    assert "--preflight-mode {strict,cached}" in readme
    assert "stateful mode" in readme
    assert "stateless mode" in readme
    assert "preflight_status.json" in readme
    assert "does not silently duplicate JSONL rows" in readme


def test_cli_docs_capture_current_mode_and_flag_contract() -> None:
    cli_doc = (REPO_ROOT / "docs/cli.md").read_text(encoding="utf-8")

    assert "--json requires --dry-run" in cli_doc
    assert "--preflight-mode {strict,cached}" in cli_doc
    assert "manifests/" in cli_doc
    assert "state/" in cli_doc
    assert "preflight_status.json" in cli_doc
    assert "can therefore appear in either execution mode" in cli_doc
    assert "safe to rerun because stateless mode rewrites the targeted output families" in cli_doc


def test_limitations_doc_matches_current_preservation_and_mode_rules() -> None:
    limitations = (REPO_ROOT / "docs/limitations.md").read_text(encoding="utf-8")

    assert "Stateful mode:" in limitations
    assert "Stateless mode:" in limitations
    assert "writes no `manifests/`" in limitations
    assert "writes no `state/`" in limitations
    assert "preflight_status.json" in limitations
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

    assert mermaid_records.__all__ == [
        "__version__",
        "__author__",
        "__license__",
        "__copyright__",
    ]
    assert hasattr(mermaid_records, "__version__")
    assert not hasattr(mermaid_records, "write_log_jsonl_prototypes")
    assert not hasattr(mermaid_records, "write_mer_jsonl_prototypes")
