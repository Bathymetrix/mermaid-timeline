from pathlib import Path
import sys

import pytest

from mermaid_timeline.bin2cycle import Bin2CycleConfig, Bin2CycleError, iter_decoded_cycle_lines


def test_iter_decoded_cycle_lines_yields_lines_from_emitted_cycle(tmp_path: Path) -> None:
    script = tmp_path / "fake_decoder.py"
    script.write_text(
        """
from pathlib import Path

class UTCDateTime:
    def __init__(self, value):
        self.value = value

def decrypt_all(path):
    return [path]

def convert_in_cycle(path, begin, end):
    workdir = Path(path)
    cycle = workdir / "0000_TEST.CYCLE"
    cycle.write_text("line one\\nline two\\n", encoding="utf-8")
""",
        encoding="utf-8",
    )
    bin_file = tmp_path / "sample.BIN"
    bin_file.write_bytes(b"raw-bin")

    config = Bin2CycleConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
    )

    lines = list(iter_decoded_cycle_lines(bin_file, config=config))

    assert lines == ["line one", "line two"]


def test_iter_decoded_cycle_lines_surfaces_subprocess_failure(tmp_path: Path) -> None:
    script = tmp_path / "fake_decoder_fail.py"
    script.write_text(
        """
raise RuntimeError("decoder boom")
""",
        encoding="utf-8",
    )
    bin_file = tmp_path / "sample.BIN"
    bin_file.write_bytes(b"raw-bin")

    config = Bin2CycleConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
    )

    with pytest.raises(Bin2CycleError, match="External decoder failed"):
        list(iter_decoded_cycle_lines(bin_file, config=config))


def test_iter_decoded_cycle_lines_requires_emitted_cycle_file(tmp_path: Path) -> None:
    script = tmp_path / "fake_decoder_no_output.py"
    script.write_text(
        """
class UTCDateTime:
    def __init__(self, value):
        self.value = value

def decrypt_all(path):
    return [path]

def convert_in_cycle(path, begin, end):
    return None
""",
        encoding="utf-8",
    )
    bin_file = tmp_path / "sample.BIN"
    bin_file.write_bytes(b"raw-bin")

    config = Bin2CycleConfig(
        python_executable=Path(sys.executable),
        decoder_script=script,
    )

    with pytest.raises(Bin2CycleError, match="did not emit any .CYCLE files"):
        list(iter_decoded_cycle_lines(bin_file, config=config))
