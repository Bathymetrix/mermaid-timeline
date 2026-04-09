# SPDX-License-Identifier: MIT

"""Prototype JSONL normalization helpers for LOG-derived operational records."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Iterable

from .models import OperationalLogEntry
from .operational_raw import iter_operational_log_entries

OUTPUT_FILENAMES = {
    "operational": "operational_records.jsonl",
    "acquisition": "acquisition_records.jsonl",
    "transmission": "transmission_records.jsonl",
    "measurement": "measurement_records.jsonl",
    "unclassified": "unclassified_operational_records.jsonl",
}

_UPLOADED_ARTIFACT_RE = re.compile(r'"(?P<artifact>[^"]+)" uploaded at (?P<rate>\d+)bytes/s')
_P_T_S_RE = re.compile(r"\bP\s*[+-]?\d+,\s*T\s*[+-]?\d+,\s*S\s*[+-]?\d+\b")
_PRESS_TEMP_RE = re.compile(r"\bP\s*[+-]?\d+mbar,\s*T\s*[+-]?\d+mdegC\b")
_BATTERY_RE = re.compile(r"\bbattery\s+(?P<mv>[+-]?\d+)mV,\s+(?P<ua>[+-]?\d+)uA\b", re.IGNORECASE)
_TRANSFER_RE = re.compile(
    r"need to transfer\s+(?P<ml>[+-]?\d+)mL\s+\(pump during\s+(?P<ms>\d+)ms\)",
    re.IGNORECASE,
)
_PUMP_RE = re.compile(r"\bpump during\s+(?P<ms>\d+)ms\b", re.IGNORECASE)
_DURATION_ONLY_RE = re.compile(r"^during\s+(?P<ms>\d+)ms$", re.IGNORECASE)
_OUTFLOW_RE = re.compile(
    r"Outflow calculated\s*:\s*(?P<value>[+-]?\d+)",
    re.IGNORECASE,
)
_PRESSURE_VALUE_RE = re.compile(r"\bP\s*(?P<pressure>[+-]?\d+)mbar\b")
_GPS_RE = re.compile(r"\bgps\b|\$GPS|GPRMC|hdop|vdop|lat|lon", re.IGNORECASE)


@dataclass(slots=True)
class LogJsonlPrototypeSummary:
    """Summary of generated prototype JSONL streams."""

    total_records: int
    operational_records: int
    acquisition_records: int
    transmission_records: int
    measurement_records: int
    unclassified_records: int
    acquisition_state_counts: dict[str, int]
    acquisition_evidence_kind_counts: dict[str, int]
    acquisition_examples: dict[str, dict[str, object]]
    transmission_examples: list[dict[str, object]]
    measurement_examples: list[dict[str, object]]
    unclassified_examples: list[dict[str, object]]
    common_unclassified_patterns: list[dict[str, object]]


def write_log_jsonl_prototypes(
    log_paths: Iterable[Path],
    output_dir: Path,
) -> LogJsonlPrototypeSummary:
    """Write conservative LOG-derived JSONL prototype streams."""

    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        name: output_dir / filename for name, filename in OUTPUT_FILENAMES.items()
    }

    total_records = 0
    operational_count = 0
    acquisition_count = 0
    transmission_count = 0
    measurement_count = 0
    unclassified_count = 0
    acquisition_state_counter: Counter[str] = Counter()
    acquisition_evidence_kind_counter: Counter[str] = Counter()
    acquisition_examples: dict[str, dict[str, object]] = {}
    transmission_examples: list[dict[str, object]] = []
    measurement_examples: list[dict[str, object]] = []
    unclassified_examples: list[dict[str, object]] = []
    unclassified_patterns: Counter[tuple[str, str | None, str]] = Counter()

    sorted_paths = sorted(Path(path) for path in log_paths)

    with (
        output_paths["operational"].open("w", encoding="utf-8") as operational_handle,
        output_paths["acquisition"].open("w", encoding="utf-8") as acquisition_handle,
        output_paths["transmission"].open("w", encoding="utf-8") as transmission_handle,
        output_paths["measurement"].open("w", encoding="utf-8") as measurement_handle,
        output_paths["unclassified"].open("w", encoding="utf-8") as unclassified_handle,
    ):
        for path in sorted_paths:
            for entry in iter_operational_log_entries(path):
                if entry.source_kind != "log":
                    continue

                total_records += 1
                acquisition_record = _classify_acquisition(entry)
                transmission_record = _classify_transmission(entry)
                measurement_record = _classify_measurement(entry)
                severity = _severity(entry.message)
                message_kind = _message_kind(
                    entry,
                    has_acquisition=acquisition_record is not None,
                    has_transmission=transmission_record is not None,
                    has_measurement=measurement_record is not None,
                )
                operational_record = {
                    "time": entry.time.isoformat(),
                    "float_id": _float_id(entry.source_file),
                    "source_container": "log",
                    "source_file": entry.source_file.as_posix(),
                    "subsystem": entry.subsystem,
                    "code": entry.code,
                    "severity": severity,
                    "message_kind": message_kind,
                    "message": entry.message,
                    "raw_line": entry.raw_line,
                }
                _write_jsonl_line(operational_handle, operational_record)
                operational_count += 1

                classified = False
                if acquisition_record is not None:
                    _write_jsonl_line(acquisition_handle, acquisition_record)
                    acquisition_count += 1
                    classified = True
                    acquisition_state_counter[
                        acquisition_record["acquisition_state"]
                    ] += 1
                    acquisition_evidence_kind_counter[
                        acquisition_record["acquisition_evidence_kind"]
                    ] += 1
                    example_key = (
                        f"{acquisition_record['acquisition_state']}:"
                        f"{acquisition_record['acquisition_evidence_kind']}"
                    )
                    acquisition_examples.setdefault(example_key, acquisition_record)

                if transmission_record is not None:
                    _write_jsonl_line(transmission_handle, transmission_record)
                    transmission_count += 1
                    classified = True
                    if len(transmission_examples) < 3:
                        transmission_examples.append(transmission_record)

                if measurement_record is not None:
                    _write_jsonl_line(measurement_handle, measurement_record)
                    measurement_count += 1
                    classified = True
                    if len(measurement_examples) < 3:
                        measurement_examples.append(measurement_record)

                if not classified:
                    unclassified_record = {
                        "time": entry.time.isoformat(),
                        "float_id": _float_id(entry.source_file),
                        "source_container": "log",
                        "source_file": entry.source_file.as_posix(),
                        "subsystem": entry.subsystem,
                        "code": entry.code,
                        "severity": severity,
                        "message": entry.message,
                        "raw_line": entry.raw_line,
                        "unclassified_reason": "no_family_match",
                    }
                    _write_jsonl_line(unclassified_handle, unclassified_record)
                    unclassified_count += 1
                    if len(unclassified_examples) < 3:
                        unclassified_examples.append(unclassified_record)
                    unclassified_patterns[
                        (entry.subsystem, entry.code, entry.message)
                    ] += 1

    common_patterns = [
        {
            "subsystem": subsystem,
            "code": code,
            "message": message,
            "count": count,
        }
        for (subsystem, code, message), count in unclassified_patterns.most_common(10)
    ]

    return LogJsonlPrototypeSummary(
        total_records=total_records,
        operational_records=operational_count,
        acquisition_records=acquisition_count,
        transmission_records=transmission_count,
        measurement_records=measurement_count,
        unclassified_records=unclassified_count,
        acquisition_state_counts=dict(acquisition_state_counter),
        acquisition_evidence_kind_counts=dict(acquisition_evidence_kind_counter),
        acquisition_examples=acquisition_examples,
        transmission_examples=transmission_examples,
        measurement_examples=measurement_examples,
        unclassified_examples=unclassified_examples,
        common_unclassified_patterns=common_patterns,
    )


def _message_kind(
    entry: OperationalLogEntry,
    *,
    has_acquisition: bool,
    has_transmission: bool,
    has_measurement: bool,
) -> str:
    if has_acquisition:
        return "acquisition"
    if has_transmission:
        return "upload"
    if has_measurement:
        return "measurement"
    message = entry.message
    lowered = message.lower()
    if _GPS_RE.search(message):
        return "gps"
    if lowered.startswith("sleep") or lowered.startswith("wake") or "timeout" in lowered:
        return "status"
    return "raw"


def _severity(message: str) -> str | None:
    if "<ERR>" in message:
        return "err"
    if "<WARN>" in message:
        return "warn"
    return None


def _classify_acquisition(entry: OperationalLogEntry) -> dict[str, object] | None:
    normalized_message = " ".join(entry.message.lower().split())
    mapping = {
        "acq started": ("started", "transition"),
        "acq stopped": ("stopped", "transition"),
        "acq already started": ("started", "assertion"),
        "acq already stopped": ("stopped", "assertion"),
    }
    details = mapping.get(normalized_message)
    if details is None:
        return None

    acquisition_state, acquisition_evidence_kind = details
    return {
        "time": entry.time.isoformat(),
        "float_id": _float_id(entry.source_file),
        "source_container": "log",
        "source_file": entry.source_file.as_posix(),
        "subsystem": entry.subsystem,
        "code": entry.code,
        "acquisition_state": acquisition_state,
        "acquisition_evidence_kind": acquisition_evidence_kind,
        "message": entry.message,
        "raw_line": entry.raw_line,
    }


def _classify_transmission(entry: OperationalLogEntry) -> dict[str, object] | None:
    message = entry.message
    if "Upload data files" in message:
        return {
            "time": entry.time.isoformat(),
            "float_id": _float_id(entry.source_file),
            "source_container": "log",
            "source_file": entry.source_file.as_posix(),
            "subsystem": entry.subsystem,
            "code": entry.code,
            "transmission_kind": "upload_batch",
            "referenced_artifact": None,
            "rate_bytes_per_s": None,
            "message": entry.message,
            "raw_line": entry.raw_line,
        }

    uploaded_match = _UPLOADED_ARTIFACT_RE.search(message)
    if uploaded_match is None:
        return None

    return {
        "time": entry.time.isoformat(),
        "float_id": _float_id(entry.source_file),
        "source_container": "log",
        "source_file": entry.source_file.as_posix(),
        "subsystem": entry.subsystem,
        "code": entry.code,
        "transmission_kind": "upload_artifact",
        "referenced_artifact": uploaded_match.group("artifact"),
        "rate_bytes_per_s": int(uploaded_match.group("rate")),
        "message": entry.message,
        "raw_line": entry.raw_line,
    }


def _classify_measurement(entry: OperationalLogEntry) -> dict[str, object] | None:
    message = entry.message
    raw_values: dict[str, str] = {}
    measurement_kind: str | None = None

    if _P_T_S_RE.search(message):
        measurement_kind = "pressure_temperature_salinity"
        raw_values["pts"] = message.strip()
    elif _PRESS_TEMP_RE.search(message):
        measurement_kind = "pressure_temperature"
        raw_values["pt"] = message.strip()
    else:
        battery_match = _BATTERY_RE.search(message)
        transfer_match = _TRANSFER_RE.search(message)
        pump_match = _PUMP_RE.search(message)
        duration_only_match = _DURATION_ONLY_RE.search(message)
        outflow_match = _OUTFLOW_RE.search(message)
        pressure_match = _PRESSURE_VALUE_RE.search(message)

        if battery_match is not None:
            measurement_kind = "battery"
            raw_values["battery_mv"] = battery_match.group("mv")
            raw_values["current_ua"] = battery_match.group("ua")
            if pressure_match is not None:
                raw_values["pressure_mbar"] = pressure_match.group("pressure")
        elif transfer_match is not None:
            measurement_kind = "transfer"
            raw_values["transfer_ml"] = transfer_match.group("ml")
            raw_values["pump_duration_ms"] = transfer_match.group("ms")
        elif pump_match is not None:
            measurement_kind = "pump_duration"
            raw_values["pump_duration_ms"] = pump_match.group("ms")
        elif duration_only_match is not None and entry.subsystem == "PUMP":
            measurement_kind = "pump_duration"
            raw_values["pump_duration_ms"] = duration_only_match.group("ms")
        elif outflow_match is not None:
            measurement_kind = "outflow"
            raw_values["outflow_raw"] = outflow_match.group("value")
        elif "mbar" in message and any(
            token in message.lower()
            for token in ("rate ", "surface ", "near ", "middle ", "far ", "ascent ", "offset")
        ):
            measurement_kind = "pressure_setting"
            raw_values["setting"] = message.strip()
        elif "mbar/s" in message and "from " in message.lower() and " to " in message.lower():
            measurement_kind = "pressure_rate"
            raw_values["transition"] = message.strip()

    if measurement_kind is None:
        return None

    return {
        "time": entry.time.isoformat(),
        "float_id": _float_id(entry.source_file),
        "source_container": "log",
        "source_file": entry.source_file.as_posix(),
        "subsystem": entry.subsystem,
        "code": entry.code,
        "measurement_kind": measurement_kind,
        "raw_values": raw_values,
        "message": entry.message,
        "raw_line": entry.raw_line,
    }


def _float_id(path: Path) -> str:
    return path.stem.split("_", maxsplit=1)[0]


def _write_jsonl_line(handle, record: dict[str, object]) -> None:
    handle.write(json.dumps(record, sort_keys=True))
    handle.write("\n")
