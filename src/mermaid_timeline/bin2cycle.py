# SPDX-License-Identifier: MIT

"""External adapter layer for upstream BIN-to-cycle decoding."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Iterator


@dataclass(slots=True)
class Bin2CycleConfig:
    """Configuration for invoking the external manufacturer decoder."""

    python_executable: Path
    decoder_script: Path


class Bin2CycleError(RuntimeError):
    """Raised when external BIN-to-cycle decoding fails."""


def iter_decoded_cycle_lines(path: Path, *, config: Bin2CycleConfig) -> Iterator[str]:
    """Yield decoded cycle text lines for one raw .BIN file.

    Observed manufacturer decoder I/O contract:

    - `preprocess.py` does not expose a stdout-oriented CLI for decoded cycle text
    - it decodes `.BIN` files into `.LOG` files and then writes `.CYCLE` files to disk
    - it may delete the source `.BIN` as part of that process

    The least invasive adapter is therefore:

    - copy the requested `.BIN` into a temporary working directory
    - invoke the external decoder in a subprocess
    - find the emitted `.CYCLE` artifact(s)
    - yield the cycle text lines
    - clean up the temporary directory afterwards
    """

    _validate_inputs(path, config)

    with tempfile.TemporaryDirectory(prefix="mermaid-bin2cycle-") as tmpdir:
        workdir = Path(tmpdir)
        bin_copy = workdir / path.name
        shutil.copy2(path, bin_copy)

        _run_decoder(workdir, config)

        cycle_paths = sorted(workdir.glob("*.CYCLE"))
        if not cycle_paths:
            raise Bin2CycleError(
                f"External decoder did not emit any .CYCLE files for {path}."
            )

        for cycle_path in cycle_paths:
            with cycle_path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    yield raw_line.rstrip("\r\n")


def _validate_inputs(path: Path, config: Bin2CycleConfig) -> None:
    """Validate adapter inputs before subprocess invocation."""

    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_file():
        raise Bin2CycleError(f"BIN input is not a file: {path}")
    if not config.python_executable.exists():
        raise FileNotFoundError(config.python_executable)
    if not config.decoder_script.exists():
        raise FileNotFoundError(config.decoder_script)


def _run_decoder(workdir: Path, config: Bin2CycleConfig) -> None:
    """Invoke the external decoder script in a subprocess."""

    harness = """
from pathlib import Path
import os
import runpy
import sys

decoder_script = Path(sys.argv[1]).resolve()
workdir = Path(sys.argv[2]).resolve()

sys.path.insert(0, str(decoder_script.parent))
namespace = runpy.run_path(str(decoder_script))

decrypt_all = namespace["decrypt_all"]
convert_in_cycle = namespace["convert_in_cycle"]
utc_class = namespace["UTCDateTime"]

workdir_str = str(workdir) + os.sep
decrypt_all(workdir_str)
convert_in_cycle(workdir_str, utc_class(0), utc_class(32503680000))
"""
    result = subprocess.run(
        [
            str(config.python_executable),
            "-c",
            harness,
            str(config.decoder_script),
            str(workdir),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit code {result.returncode}"
        raise Bin2CycleError(f"External decoder failed: {detail}")
