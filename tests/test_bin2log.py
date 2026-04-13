# SPDX-License-Identifier: MIT

from pathlib import Path
import sys

import pytest

from mermaid_records.bin2log import (
    Bin2LogConfig,
    Bin2LogError,
    decode_workspace_logs,
    iter_decoded_log_lines,
    prepare_decode_workspace,
)


def test_iter_decoded_log_lines_yields_lines_from_emitted_log(tmp_path: Path) -> None:
    script = tmp_path / "fake_decoder.py"
    script.write_text(
        """
from pathlib import Path

def decrypt_all(path):
    workdir = Path(path)
    log = workdir / "0000_TEST.LOG"
    log.write_text("line one\\nline two\\n", encoding="utf-8")
    return [path]
""",
        encoding="utf-8",
    )
    bin_file = tmp_path / "sample.BIN"
    bin_file.write_bytes(b"raw-bin")

    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
    )

    lines = list(iter_decoded_log_lines(bin_file, config=config))

    assert lines == ["line one", "line two"]


def test_iter_decoded_log_lines_surfaces_subprocess_failure(tmp_path: Path) -> None:
    script = tmp_path / "fake_decoder_fail.py"
    script.write_text(
        """
raise RuntimeError("decoder boom")
""",
        encoding="utf-8",
    )
    bin_file = tmp_path / "sample.BIN"
    bin_file.write_bytes(b"raw-bin")

    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
    )

    with pytest.raises(Bin2LogError, match="External decoder failed"):
        list(iter_decoded_log_lines(bin_file, config=config))


def test_iter_decoded_log_lines_requires_emitted_log_file(tmp_path: Path) -> None:
    script = tmp_path / "fake_decoder_no_output.py"
    script.write_text(
        """
def decrypt_all(path):
    return [path]
""",
        encoding="utf-8",
    )
    bin_file = tmp_path / "sample.BIN"
    bin_file.write_bytes(b"raw-bin")

    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
    )

    with pytest.raises(Bin2LogError, match="did not emit any .LOG files"):
        list(iter_decoded_log_lines(bin_file, config=config))


def test_prepare_decode_workspace_strict_mode_fails_on_database_update_error(
    tmp_path: Path,
) -> None:
    script = _write_database_warning_decoder(tmp_path)
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
        preflight_mode="strict",
    )

    with pytest.raises(Bin2LogError, match="External decoder database update failed"):
        prepare_decode_workspace(workdir, config=config, refresh_database=True)


def test_prepare_decode_workspace_default_mode_is_strict(tmp_path: Path) -> None:
    script = _write_database_warning_decoder(tmp_path)
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
    )

    with pytest.raises(Bin2LogError, match="External decoder database update failed"):
        prepare_decode_workspace(workdir, config=config, refresh_database=True)


def test_prepare_decode_workspace_cached_mode_warns_and_decode_continues(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    script = _write_database_warning_decoder(tmp_path)
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    bin_file = workdir / "0000_TEST.BIN"
    bin_file.write_bytes(b"raw-bin")

    config = Bin2LogConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
        preflight_mode="cached",
    )

    prepare_decode_workspace(workdir, config=config, refresh_database=True)
    log_paths = decode_workspace_logs(workdir, config=config)

    captured = capsys.readouterr()
    assert "WARNING: database_update failed; continuing in cached preflight mode" in captured.err
    assert [path.name for path in log_paths] == ["0000_TEST.LOG"]


def test_bin2log_config_rejects_unknown_preflight_mode(tmp_path: Path) -> None:
    script = tmp_path / "fake_decoder.py"
    script.write_text("def decrypt_all(path):\n    return [path]\n", encoding="utf-8")

    with pytest.raises(ValueError, match="preflight_mode must be one of"):
        Bin2LogConfig(
            python_executable=Path(sys.executable),
            decoder_script=script,
            preflight_mode="auto",  # type: ignore[arg-type]
        )


def _write_database_warning_decoder(tmp_path: Path) -> Path:
    script = tmp_path / "fake_decoder_database_warning.py"
    script.write_text(
        """
from pathlib import Path

def database_update(_arg):
    print("Update Databases")
    print('Exception: "simulated refresh failure" detected when get Databases.json')

def concatenate_files(path):
    return [path]

def concatenate_rbr_files(path):
    return [path]

def decrypt_all(path):
    workdir = Path(path)
    log = workdir / "0000_TEST.LOG"
    log.write_text("line one\\nline two\\n", encoding="utf-8")
    return [path]
""",
        encoding="utf-8",
    )
    return script
