# SPDX-License-Identifier: MIT

from pathlib import Path

from mermaid_timeline.audit import (
    audit_processed_cycle,
    audit_processed_cycle_h,
    audit_server_mer,
)


def test_audit_server_mer_counts_empty_and_non_empty_files(tmp_path: Path) -> None:
    root = tmp_path / "server"
    root.mkdir()
    _write_mer(
        root / "0001_ABCDEF01.MER",
        event_payloads=[],
    )
    _write_mer(
        root / "0002_ABCDEF02.MER",
        event_payloads=[b"\x01\x02\x03"],
    )

    stats = audit_server_mer(root)

    assert stats.total_files == 2
    assert stats.parsed_ok == 2
    assert stats.empty_files == 1
    assert stats.non_empty_files == 1


def test_audit_processed_cycle_counts_parse_failures(tmp_path: Path) -> None:
    root = tmp_path / "processed"
    root.mkdir()
    (root / "0001_ABCDEF01.CYCLE.h").write_text(
        "2025-01-01T00:00:00:[PREPROCESS]Create 0001_ABCDEF01.LOG\n",
        encoding="utf-8",
    )
    (root / "0002_ABCDEF02.CYCLE.h").write_text(
        "[MRMAID,1970-01-01T00:00:56:[MRMAID,1970-01-01T00:00:565]1520dbar, -11degC\n",
        encoding="utf-8",
    )

    stats = audit_processed_cycle(root)

    assert stats.total_files == 2
    assert stats.parsed_ok == 1
    assert stats.parse_failures == 1


def test_audit_processed_cycle_h_alias_counts_parse_failures(tmp_path: Path) -> None:
    root = tmp_path / "processed"
    root.mkdir()
    (root / "0001_ABCDEF01.CYCLE.h").write_text(
        "2025-01-01T00:00:00:[PREPROCESS]Create 0001_ABCDEF01.LOG\n",
        encoding="utf-8",
    )

    stats = audit_processed_cycle_h(root)

    assert stats.total_files == 1
    assert stats.parsed_ok == 1
    assert stats.parse_failures == 0


def _write_mer(path: Path, *, event_payloads: list[bytes]) -> None:
    environment = (
        "<ENVIRONMENT>\n"
        "\t<BOARD 452116600-01 />\n"
        "\t<SOFTWARE 2.1344 />\n"
        "\t<DIVE ID=1 EVENTS=0 />\n"
        "\t<POOL EVENTS=0 SIZE=0 />\n"
        "\t<SAMPLE MIN=0 MAX=0 />\n"
        "\t<TRUE_SAMPLE_FREQ FS_Hz=40.0 />\n"
        "</ENVIRONMENT>\n"
    )
    parameters = "<PARAMETERS>\n\t<MISC UPLOAD_MAX=100kB />\n</PARAMETERS>"
    events = []
    for payload in event_payloads:
        events.append(
            (
                "<EVENT>\n"
                "\t<INFO DATE=2025-01-01T00:00:00 PRESSURE=1.0 TEMPERATURE=2.0 "
                "CRITERION=3.0 SNR=4.0 TRIG=5 DETRIG=6 />\n"
                "\t<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.0 "
                "STAGES=1 NORMALIZED=YES LENGTH=1 />\n"
                "\t<DATA>"
            ).encode("ascii")
            + payload
            + b"</DATA>\n</EVENT>\n"
        )
    path.write_bytes(environment.encode("ascii") + parameters.encode("ascii") + b"".join(events))
