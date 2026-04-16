# SPDX-License-Identifier: MIT

"""LOG-to-JSONL normalization helpers for prototype record-family outputs."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Iterable

from .models import OperationalLogEntry
from .operational_raw import iter_operational_log_entries
from .parse_float_name import maybe_parse_float_name

OUTPUT_FILENAMES = {
    "operational": "log_operational_records.jsonl",
    "acquisition": "log_acquisition_records.jsonl",
    "ascent_request": "log_ascent_request_records.jsonl",
    "gps": "log_gps_records.jsonl",
    "transmission": "log_transmission_records.jsonl",
    "measurement": "log_measurement_records.jsonl",
    "unclassified": "log_unclassified_records.jsonl",
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
_GPS_POSITION_RE = re.compile(
    r"(?P<latitude>[NS]\d+deg\d+(?:\.\d+)?mn)\s*,\s*(?P<longitude>[EW]\d+deg\d+(?:\.\d+)?mn)"
)
_HDOP_RE = re.compile(r"\bhdop\s+(?P<hdop>[+-]?\d+(?:\.\d+)?)", re.IGNORECASE)
_VDOP_RE = re.compile(r"\bvdop\s+(?P<vdop>[+-]?\d+(?:\.\d+)?)", re.IGNORECASE)
_GPSACK_RE = re.compile(r"\$?GPSACK:(?P<payload>[^;]+)")
_GPSOFF_RE = re.compile(r"\$?GPSOFF:(?P<offset>[+-]?\d+)")


@dataclass(slots=True)
class LogJsonlPrototypeSummary:
    """Summary of generated prototype JSONL streams."""

    total_records: int
    operational_records: int
    acquisition_records: int
    ascent_request_records: int
    gps_records: int
    transmission_records: int
    measurement_records: int
    unclassified_records: int
    acquisition_state_counts: dict[str, int]
    acquisition_evidence_kind_counts: dict[str, int]
    acquisition_examples: dict[str, dict[str, object]]
    ascent_request_state_counts: dict[str, int]
    ascent_request_examples: dict[str, dict[str, object]]
    gps_record_kind_counts: dict[str, int]
    gps_examples: dict[str, dict[str, object]]
    transmission_examples: list[dict[str, object]]
    measurement_examples: list[dict[str, object]]
    unclassified_examples: list[dict[str, object]]
    common_unclassified_patterns: list[dict[str, object]]


def _common_log_record_fields(
    entry: OperationalLogEntry,
    *,
    instrument_id: str,
) -> dict[str, object]:
    """Return shared provenance and source fields for LOG-derived records."""

    return {
        "instrument_id": instrument_id,
        "source_file": entry.source_file.as_posix(),
        "source_container": "log",
        "record_time": entry.time.isoformat(),
        "log_epoch_time": _log_epoch_time(entry),
        "subsystem": entry.subsystem,
        "code": entry.code,
        "message": entry.message,
    }


def write_log_jsonl_prototypes(
    log_paths: Iterable[Path],
    output_dir: Path,
    *,
    instrument_id: str | None = None,
    run_id: str | None = None,
    malformed_log_lines: list[dict[str, object]] | None = None,
    skipped_log_files: list[dict[str, object]] | None = None,
) -> LogJsonlPrototypeSummary:
    """Write conservative LOG-derived JSONL prototype streams."""

    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        name: output_dir / filename for name, filename in OUTPUT_FILENAMES.items()
    }

    total_records = 0
    operational_count = 0
    acquisition_count = 0
    ascent_request_count = 0
    gps_count = 0
    transmission_count = 0
    measurement_count = 0
    unclassified_count = 0
    acquisition_state_counter: Counter[str] = Counter()
    acquisition_evidence_kind_counter: Counter[str] = Counter()
    acquisition_examples: dict[str, dict[str, object]] = {}
    ascent_request_state_counter: Counter[str] = Counter()
    ascent_request_examples: dict[str, dict[str, object]] = {}
    gps_record_kind_counter: Counter[str] = Counter()
    gps_examples: dict[str, dict[str, object]] = {}
    transmission_examples: list[dict[str, object]] = []
    measurement_examples: list[dict[str, object]] = []
    unclassified_examples: list[dict[str, object]] = []
    unclassified_patterns: Counter[tuple[str, str | None, str]] = Counter()

    sorted_paths = sorted(Path(path) for path in log_paths)

    with (
        output_paths["operational"].open("w", encoding="utf-8") as operational_handle,
        output_paths["acquisition"].open("w", encoding="utf-8") as acquisition_handle,
        output_paths["ascent_request"].open("w", encoding="utf-8") as ascent_request_handle,
        output_paths["gps"].open("w", encoding="utf-8") as gps_handle,
        output_paths["transmission"].open("w", encoding="utf-8") as transmission_handle,
        output_paths["measurement"].open("w", encoding="utf-8") as measurement_handle,
        output_paths["unclassified"].open("w", encoding="utf-8") as unclassified_handle,
    ):
        for path in sorted_paths:
            path_instrument_id = instrument_id or _fallback_instrument_id(path)
            def _record_malformed_line(
                line_number: int,
                raw_line: str,
                error: str,
            ) -> None:
                if malformed_log_lines is None or run_id is None:
                    return
                malformed_log_lines.append(
                    {
                        "run_id": run_id,
                        "instrument_id": path_instrument_id,
                        "source_file": path.as_posix(),
                        "line_number": line_number,
                        "raw_line": raw_line,
                        "error": error,
                    }
                )
            try:
                for entry in iter_operational_log_entries(
                    path,
                    on_malformed_line=_record_malformed_line,
                ):
                    if entry.source_kind != "log":
                        continue

                    total_records += 1
                    acquisition_record = _classify_acquisition(entry, instrument_id=path_instrument_id)
                    ascent_request_record = _classify_ascent_request(entry, instrument_id=path_instrument_id)
                    gps_record = _classify_gps(entry, instrument_id=path_instrument_id)
                    transmission_record = _classify_transmission(entry, instrument_id=path_instrument_id)
                    measurement_record = _classify_measurement(entry, instrument_id=path_instrument_id)
                    severity = _severity(entry.message)
                    message_kind = _message_kind(
                        entry,
                        has_acquisition=acquisition_record is not None,
                        has_ascent_request=ascent_request_record is not None,
                        has_gps=gps_record is not None,
                        has_transmission=transmission_record is not None,
                        has_measurement=measurement_record is not None,
                    )
                    common_fields = _common_log_record_fields(entry, instrument_id=path_instrument_id)
                    operational_record = {
                        **common_fields,
                        "severity": severity,
                        "message_kind": message_kind,
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

                    if ascent_request_record is not None:
                        _write_jsonl_line(ascent_request_handle, ascent_request_record)
                        ascent_request_count += 1
                        classified = True
                        ascent_request_state_counter[
                            ascent_request_record["ascent_request_state"]
                        ] += 1
                        ascent_request_examples.setdefault(
                            ascent_request_record["ascent_request_state"],
                            ascent_request_record,
                        )

                    if gps_record is not None:
                        _write_jsonl_line(gps_handle, gps_record)
                        gps_count += 1
                        classified = True
                        gps_record_kind_counter[gps_record["gps_record_kind"]] += 1
                        gps_examples.setdefault(gps_record["gps_record_kind"], gps_record)

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
                            **common_fields,
                            "severity": severity,
                            "unclassified_reason": "no_family_match",
                            "raw_line": entry.raw_line,
                        }
                        _write_jsonl_line(unclassified_handle, unclassified_record)
                        unclassified_count += 1
                        if len(unclassified_examples) < 3:
                            unclassified_examples.append(unclassified_record)
                        unclassified_patterns[
                            (entry.subsystem, entry.code, entry.message)
                        ] += 1
            except OSError as exc:
                if skipped_log_files is None or run_id is None:
                    raise
                skipped_log_files.append(
                    {
                        "run_id": run_id,
                        "instrument_id": path_instrument_id,
                        "source_file": path.as_posix(),
                        "error": str(exc),
                        "skipped_at": _iso_now(),
                    }
                )
                continue
            except Exception as exc:
                raise ValueError(f"Error while normalizing LOG file {path}: {exc}") from exc

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
        ascent_request_records=ascent_request_count,
        gps_records=gps_count,
        transmission_records=transmission_count,
        measurement_records=measurement_count,
        unclassified_records=unclassified_count,
        acquisition_state_counts=dict(acquisition_state_counter),
        acquisition_evidence_kind_counts=dict(acquisition_evidence_kind_counter),
        acquisition_examples=acquisition_examples,
        ascent_request_state_counts=dict(ascent_request_state_counter),
        ascent_request_examples=ascent_request_examples,
        gps_record_kind_counts=dict(gps_record_kind_counter),
        gps_examples=gps_examples,
        transmission_examples=transmission_examples,
        measurement_examples=measurement_examples,
        unclassified_examples=unclassified_examples,
        common_unclassified_patterns=common_patterns,
    )


def _message_kind(
    entry: OperationalLogEntry,
    *,
    has_acquisition: bool,
    has_ascent_request: bool,
    has_gps: bool,
    has_transmission: bool,
    has_measurement: bool,
) -> str:
    if has_acquisition:
        return "acquisition"
    if has_ascent_request:
        return "status"
    if has_gps:
        return "gps"
    if has_transmission:
        return "upload"
    if has_measurement:
        return "measurement"
    message = entry.message
    lowered = message.lower()
    if lowered.startswith("sleep") or lowered.startswith("wake") or "timeout" in lowered:
        return "status"
    if _GPS_RE.search(message):
        return "gps"
    return "raw"


def _severity(message: str) -> str | None:
    if "<ERR>" in message:
        return "err"
    if "<WARN>" in message:
        return "warn"
    return None


def _classify_acquisition(
    entry: OperationalLogEntry,
    *,
    instrument_id: str,
) -> dict[str, object] | None:
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
        **_common_log_record_fields(entry, instrument_id=instrument_id),
        "acquisition_state": acquisition_state,
        "acquisition_evidence_kind": acquisition_evidence_kind,
        "raw_line": entry.raw_line,
    }


def _classify_ascent_request(
    entry: OperationalLogEntry,
    *,
    instrument_id: str,
) -> dict[str, object] | None:
    normalized_message = " ".join(entry.message.lower().split())
    mapping = {
        "ascent request accepted": "accepted",
        "ascent request rejected": "rejected",
    }
    ascent_request_state = mapping.get(normalized_message)
    if ascent_request_state is None:
        return None

    return {
        **_common_log_record_fields(entry, instrument_id=instrument_id),
        "ascent_request_state": ascent_request_state,
        "raw_line": entry.raw_line,
    }


def _classify_gps(entry: OperationalLogEntry, *, instrument_id: str) -> dict[str, object] | None:
    message = entry.message.strip()
    gps_record_kind: str | None = None
    raw_values: dict[str, str] | None = None

    if "GPS fix..." in message:
        gps_record_kind = "fix_attempt"
    else:
        position_match = _GPS_POSITION_RE.search(message)
        hdop_match = _HDOP_RE.search(message)
        vdop_match = _VDOP_RE.search(message)
        gpsack_match = _GPSACK_RE.search(message)
        gpsoff_match = _GPSOFF_RE.search(message)

        if position_match is not None:
            gps_record_kind = "fix_position"
            raw_values = {
                "latitude": position_match.group("latitude"),
                "longitude": position_match.group("longitude"),
            }
        elif hdop_match is not None or vdop_match is not None:
            gps_record_kind = "dop"
            raw_values = {}
            if hdop_match is not None:
                raw_values["hdop"] = hdop_match.group("hdop")
            if vdop_match is not None:
                raw_values["vdop"] = vdop_match.group("vdop")
        elif gpsack_match is not None:
            gps_record_kind = "gps_ack"
            raw_values = {"gpsack": gpsack_match.group("payload")}
        elif gpsoff_match is not None:
            gps_record_kind = "gps_off"
            raw_values = {"gpsoff": gpsoff_match.group("offset")}

    if gps_record_kind is None:
        return None

    return {
        **_common_log_record_fields(entry, instrument_id=instrument_id),
        "gps_record_kind": gps_record_kind,
        "raw_values": raw_values,
        "raw_line": entry.raw_line,
    }


def _classify_transmission(
    entry: OperationalLogEntry,
    *,
    instrument_id: str,
) -> dict[str, object] | None:
    message = entry.message
    if "Upload data files" in message:
        return {
            **_common_log_record_fields(entry, instrument_id=instrument_id),
            "transmission_kind": "upload_batch",
            "referenced_artifact": None,
            "rate_bytes_per_s": None,
            "raw_line": entry.raw_line,
        }

    uploaded_match = _UPLOADED_ARTIFACT_RE.search(message)
    if uploaded_match is None:
        return None

    return {
        **_common_log_record_fields(entry, instrument_id=instrument_id),
        "transmission_kind": "upload_artifact",
        "referenced_artifact": uploaded_match.group("artifact"),
        "rate_bytes_per_s": int(uploaded_match.group("rate")),
        "raw_line": entry.raw_line,
    }


def _classify_measurement(
    entry: OperationalLogEntry,
    *,
    instrument_id: str,
) -> dict[str, object] | None:
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
        **_common_log_record_fields(entry, instrument_id=instrument_id),
        "measurement_kind": measurement_kind,
        "raw_values": raw_values,
        "raw_line": entry.raw_line,
    }


def _fallback_instrument_id(path: Path) -> str:
    for candidate in (path.parent.name, path.stem):
        parsed = maybe_parse_float_name(candidate)
        if parsed is not None:
            return parsed.instrument_id
    return path.stem.split("_", maxsplit=1)[0]


def _log_epoch_time(entry: OperationalLogEntry) -> str:
    return entry.raw_line.split(":", maxsplit=1)[0]


def _write_jsonl_line(handle, record: dict[str, object]) -> None:
    handle.write(json.dumps(record))
    handle.write("\n")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()
