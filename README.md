[![Tests](https://github.com/Bathymetrix/mermaid-records/actions/workflows/ci.yml/badge.svg)](https://github.com/Bathymetrix/mermaid-records/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.14%2B-blue.svg)](https://pypi.org/project/mermaid-records/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)

# mermaid-records

`mermaid-records` normalizes raw MERMAID `BIN`, `LOG`, and `MER` artifacts into structured JSONL record families without introducing interpretation.

It is a normalization layer, not an interpretation or analysis layer. The package restructures source artifacts conservatively, preserves source information where practical, and leaves higher-level derivation, interval inference, coordinate conversion, and scientific interpretation to downstream tooling.

## Scope

`mermaid-records` is intended for scientists and operators who work with MERMAID float data and need a stable, machine-readable baseline from raw artifacts.

Version 1 focuses on truthful normalization:

- normalize raw `BIN`, `LOG`, and `MER` inputs into canonical JSONL record families
- preserve source information conservatively
- avoid silent data loss during parsing and normalization
- keep interpretation to a minimum
- make malformed or non-normalizable content visible rather than silently dropping it

## Non-goals

This package intentionally does **not**:

- convert raw latitude/longitude strings into decimal degrees
- derive acquisition, recording, or transmission intervals beyond explicit normalized evidence
- infer higher-level mission or scientific meaning
- perform waveform analysis
- replace manufacturer decoding logic

Small explicit normalizations may still occur where required for stable structured output. See `docs/limitations.md` for the allowed transformations called out explicitly in the v1 contract.

## Decoder boundary

`LOG` and `MER` normalization are first-class package behavior.

`BIN` inputs require an external preprocess/decode step to produce `LOG` artifacts. In v1, that decoder remains external and is invoked across a subprocess boundary rather than reimplemented inside this package. This is deliberate: `mermaid-records` owns normalization, not vendor decoder logic.

## Installation

```bash
python -m pip install .
```

For development:

```bash
python -m pip install -e .[dev]
```

## CLI

The installed entrypoint is:

```bash
mermaid-records normalize --input-root <INPUT_ROOT> --output-dir <OUTPUT_DIR>
```

Supported normalize flags currently include:

- `--input-root` for stateful mode
- `--input-file` for stateless mode
- `--output-dir`
- `--decoder-python` and `--decoder-script` for BIN-backed runs
- `--preflight-mode {strict,cached}`
- `--dry-run`
- `--force-rewrite`
- `--json` for structured dry-run output
- `--verbose` / `-v`

`--output-dir` may also resolve from `$MERMAID/records` when `--output-dir` is omitted and `MERMAID` is set.

The CLI has both stateful mode and stateless mode. See `docs/cli.md` for the authoritative interface and mode details.

In v1, stateless reruns are rewrite-only for the targeted instrument outputs. They do not use append/noop incremental behavior, so rerunning the same explicit inputs does not silently duplicate JSONL rows.

## Output model

Normalization writes per-instrument JSONL record families under the selected output directory.

Typical families include:

```text
<output-dir>/
  <instrument>/
    log_acquisition_records.jsonl
    log_ascent_request_records.jsonl
    log_battery_records.jsonl
    log_gps_records.jsonl
    log_operational_records.jsonl
    log_parameter_records.jsonl
    log_pressure_temperature_records.jsonl
    log_sbe_records.jsonl
    log_testmode_records.jsonl
    log_transmission_records.jsonl
    log_unclassified_records.jsonl
    mer_environment_records.jsonl
    mer_event_records.jsonl
    mer_parameter_records.jsonl
    manifests/          # stateful mode only
    state/              # stateful mode only
    preflight_status.json  # only when BIN decode preflight ran
```

Not every float emits every record family, and not every family populates every optional field. Presence, absence, and sparsity reflect the underlying source artifacts and float generation, not necessarily a normalization defect.

The tracked fixture families currently used for release-facing examples and tests are:

- `452.020-P-06`
- `467.174-T-0100`

No single float should be expected to exercise every family or every non-null field.

## Representative normalized records

### LOG acquisition

```json
{"instrument_id":"T0100","source_file":"0100_6492BBF7.LOG","source_container":"log","record_time":"2023-06-21T16:55:34","log_epoch_time":"1687366534","subsystem":"MRMAID","code":"0002","message":"acq started","acquisition_state":"started","acquisition_evidence_kind":"transition","raw_line":"1687366534:[MRMAID,0002]acq started"}
```

### LOG GPS

```json
{"instrument_id":"P0006","source_file":"06_67D03E13.LOG","source_container":"log","record_time":"2025-03-11T13:44:29","log_epoch_time":"1741700669","subsystem":"SURF","code":"394","message":"S13deg22.967mn, W173deg28.554mn","gps_record_kind":"fix_position","raw_values":{"latitude":"S13deg22.967mn","longitude":"W173deg28.554mn"},"raw_line":"1741700669:[SURF ,394]S13deg22.967mn, W173deg28.554mn"}
```

### LOG parameter episode

```json
{"instrument_id":"P0006","source_file":"06_67C95E46.LOG","episode_index":0,"start_record_time":"2025-03-06T08:39:06","end_record_time":"2025-03-06T08:39:06","raw_lines":["1741250346: bypass 10000ms 10000ms (10000ms 200000ms stored)","1741250346: valve 60000ms 12750 (60000ms 12750 stored)","1741250346: pump 60000ms 30% 10750 80% (60000ms 30% 10750 80% stored)","...","1741250346: stage[0] 0mbar (+/-5000mbar) 86400s (<86400s)"]}
```

### LOG transmission

```json
{"instrument_id":"T0100","source_file":"0100_66E74070.LOG","source_container":"log","record_time":"2024-09-15T16:22:53","log_epoch_time":"1726434973","subsystem":"UPLOAD","code":"0231","message":"\"0100/66E73F16.MER\" uploaded at 80bytes/s","referenced_artifact":"0100_66E73F16.MER","rate_bytes_per_s":80,"raw_line":"1726434973:[UPLOAD,0231]\"0100/66E73F16.MER\" uploaded at 80bytes/s","transmission_kind":"upload_artifact"}
```

### MER environment

```json
{"instrument_id":"T0100","source_file":"0100_68B47F96.MER","source_container":"mer","environment_kind":"gpsinfo","gpsinfo_date":"2025-08-21T17:00:37","raw_values":{"date":"2025-08-21T17:00:37","lat":"+3218.9010","lon":"+13524.7830"},"line":"\t<GPSINFO DATE=2025-08-21T17:00:37 LAT=+3218.9010 LON=+13524.7830 />"}
```

### MER event

```json
{"instrument_id":"P0006","source_file":"06_6799729E.MER","source_container":"mer","block_index":0,"event_index":0,"event_info_date":"2025-01-28T11:26:29.948059","pressure":"763.00","temperature":"-11.0000","criterion":"0.0478937","snr":"5.498","trig":"2000","detrig":"5733","endianness":"LITTLE","bytes_per_sample":"4","sampling_rate":"20.000000","stages":"5","normalized":"YES","length":"4736","raw_info_line":"<INFO DATE=2025-01-28T11:26:29.948059 PRESSURE=763.00 TEMPERATURE=-11.0000 CRITERION=0.0478937 SNR=5.498 TRIG=2000 DETRIG=5733 />","raw_format_line":"<FORMAT ENDIANNESS=LITTLE BYTES_PER_SAMPLE=4 SAMPLING_RATE=20.000000 STAGES=5 NORMALIZED=YES LENGTH=4736 />","encoded_payload":"<base64 omitted>"}
```

### MER parameter

```json
{"instrument_id":"T0100","source_file":"0100_6492BBAB.MER","source_container":"mer","parameter_kind":"misc","raw_values":{"upload_max":"100kB"},"line":"\t<MISC UPLOAD_MAX=100kB />"}
```

## Source preservation

`mermaid-records` preserves source information in three main ways:

- directly in normalized records, for example `raw_line`, `raw_lines`, or `line`
- as structured components, for example `raw_info_line`, `raw_format_line`, and encoded payload fields in MER event records
- in stateful audit/manifests when that mode is enabled
- in `preflight_status.json` when BIN decode preflight runs with a durable output directory

This package does **not** guarantee that every JSONL record contains a full verbatim copy of the original source block.

In particular, some normalized records, notably MER event records, do not preserve the original `<EVENT>...</EVENT>` block verbatim. Instead, they preserve structured components sufficient for downstream interpretation, but not byte-for-byte reconstruction of the full original block.

## Documentation map

- `docs/cli.md` — CLI interface, execution modes, and rewrite behavior
- `docs/ethos.md` — design philosophy and scope discipline
- `docs/limitations.md` — explicit limitations, preservation caveats, and allowed transformations
- `docs/notes/normalization_release_contract.md` — detailed behavioral contract and reference semantics

## Design constraint

> The scope of this package should not be allowed to creep.

New formats and edge cases may extend normalization coverage, but features that introduce interpretation, derived semantics, or higher-level analysis should not be added here.

© 2026 Bathymetrix, LLC
