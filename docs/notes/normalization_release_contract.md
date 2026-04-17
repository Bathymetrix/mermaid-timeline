# SPDX-License-Identifier: MIT

# Normalization Release Contract

This document is the release-facing contract for `mermaid-records` before `v1.0.0`.
It describes the behavior that downstream callers, fixture audits, and future hardening work should treat as stable unless a deliberate breaking change is made.

## Scope

`mermaid-records` is scoped strictly to canonical normalization of raw MERMAID data:

- external `BIN -> LOG` decode
- `LOG -> JSONL`
- `MER -> JSONL`

Supported raw input file types for `v1.0.0` are intentionally limited to:

- `BIN`
- `LOG`
- `MER`

Explicitly not supported for `v1.0.0`:

- `S41`
- `S61`
- `RBR`
- other profile or auxiliary formats

Out of scope:

- analysis or timeline interpretation
- unit or coordinate conversions
- inferred durations from sample counts or rates
- inferred acquisition windows beyond explicit transition lines
- DET / REQ logic
- `CYCLE` / `CYCLE.h`

## Public CLI

Installed CLI surface:

- `mermaid-records normalize`

The `normalize` subcommand accepts exactly one input mode selector:

- `--input-root`
  - stateful mode
- `--input-file`
  - stateless mode

Shared options:

- `--output-dir`
- `--decoder-python`
- `--decoder-script`
- `--preflight-mode {strict,cached}`
- `--dry-run`
- `--json`

`--decoder-python` and `--decoder-script` must be supplied together when any `BIN` inputs are present.

## Execution Modes

### Stateful

Stateful mode is triggered by `--input-root`.

Contract:

- manifests are read and written
- incremental rerun logic is enabled
- pruning is enabled
- instrument output directories use full serial names from `<serial>.vit` when available
- dry-run reuses the same planning and diff logic but writes nothing

### Stateless

Stateless mode is triggered by `--input-file`.

Contract:

- input is an explicit raw file list
- manifests are ignored and not written
- incremental rerun logic is disabled
- pruning is disabled
- the target output tree must not already contain `manifests/`
- dry-run writes no outputs, manifests, state files, or status files

## BIN Decode Preflight

BIN decode preflight has exactly two requested modes:

- `strict`
  - default
  - live `database_update(...)` refresh must succeed
  - failure is terminal
- `cached`
  - live `database_update(...)` refresh is still attempted
  - refresh failure may continue in degraded cached mode

When a durable output directory is used, `preflight_status.json` records:

- requested mode
- effective mode
- whether the refresh was attempted
- whether it succeeded
- whether the run continued after failure
- failure detail, if any
- decoder executable/script identity and write time

## Instrument Identity

Canonical `instrument_id` resolution is centralized in `src/mermaid_records/parse_instrument_name.py`.

Examples:

- `452.020-P-08` -> `instrument_id = P0008`
- `467.174-T-0100` -> `instrument_id = T0100`

When a full serial is unavailable, the pipeline falls back conservatively to the raw file prefix.

## Output Layout

Per instrument:

```text
<output_root>/<instrument-serial>/
  log_operational_records.jsonl
  log_acquisition_records.jsonl
  log_ascent_request_records.jsonl
  log_gps_records.jsonl
  log_pressure_temperature_records.jsonl
  log_battery_records.jsonl
  log_parameter_records.jsonl
  log_testmode_records.jsonl
  log_sbe_records.jsonl
  log_transmission_records.jsonl
  log_unclassified_records.jsonl
  mer_environment_records.jsonl
  mer_parameter_records.jsonl
  mer_event_records.jsonl
  preflight_status.json
  manifests/
    latest.json
    runs/
      <run_id>/
        run.json
        outputs.json
        source_state.json
        input_file_diffs.jsonl
        preflight_status.json
  state/
    pruned_records.jsonl
```

Notes:

- JSONL field ordering is frozen semantically: provenance/identity, then time, then family metadata, then payload/accounting, then raw fallback fields.
- `preflight_status.json` at instrument root is present only when BIN preflight ran with a durable output directory.
- `latest.json` points to the most recent run for that instrument.
- `run.json` stores run metadata and status.
- `outputs.json` stores output inventory and row counts.
- `source_state.json` stores raw source identity and decoder-state identity.
- `input_file_diffs.jsonl` stores one row per raw source file with file-level diff fields only.
- `state/pruned_records.jsonl` stores removed-source observations from stateful reruns.

## JSONL Filenames

LOG families:

- `log_operational_records.jsonl`
- `log_acquisition_records.jsonl`
- `log_ascent_request_records.jsonl`
- `log_gps_records.jsonl`
- `log_pressure_temperature_records.jsonl`
- `log_battery_records.jsonl`
- `log_parameter_records.jsonl`
- `log_testmode_records.jsonl`
- `log_sbe_records.jsonl`
- `log_transmission_records.jsonl`
- `log_unclassified_records.jsonl`

MER families:

- `mer_environment_records.jsonl`
- `mer_parameter_records.jsonl`
- `mer_event_records.jsonl`

## JSONL Record Schemas

### LOG

`log_operational_records.jsonl`

- `source_file` is basename-only in normalized JSONL outputs
- `record_time`
- `log_epoch_time`
- `instrument_id`
- `source_container`
- `source_file`
- `subsystem`
- `code`
- `message`
- `raw_line`
- `severity`
- `message_kind`
- `switched_to_log_file` (only for parsed rollover banner rows such as `*** switching to ... ***`)

`log_acquisition_records.jsonl`

- all operational provenance/source fields
- `acquisition_state`
- `acquisition_evidence_kind`

`log_ascent_request_records.jsonl`

- all operational provenance/source fields
- `ascent_request_state`

`log_gps_records.jsonl`

- all operational provenance/source fields
- `gps_record_kind`
- `raw_values`

`log_parameter_records.jsonl`

- grouped startup/dive-parameter episodes preserved from LOG continuation lines
- `instrument_id`
- `source_file`
- `episode_index`
- `line_start_index`
- `line_end_index`
- `start_record_time`
- `end_record_time`
- `start_log_epoch_time`
- `end_log_epoch_time`
- `raw_lines`

`log_testmode_records.jsonl`

- grouped test-mode sessions preserved from LOGs
- `instrument_id`
- `source_file`
- `episode_index`
- `line_start_index`
- `line_end_index`
- `start_record_time`
- `end_record_time`
- `start_log_epoch_time`
- `end_log_epoch_time`
- `raw_lines`

`log_sbe_records.jsonl`

- grouped SBE/profil operational episodes preserved from LOGs
- `instrument_id`
- `source_file`
- `episode_index`
- `line_start_index`
- `line_end_index`
- `start_record_time`
- `end_record_time`
- `start_log_epoch_time`
- `end_log_epoch_time`
- `raw_lines`

`log_transmission_records.jsonl`

- all operational provenance/source fields
- `transmission_kind`
- `referenced_artifact` (`/` normalized to `_` when the LOG text is parsed as a LOG/MER filename reference)
- `rate_bytes_per_s`

`log_pressure_temperature_records.jsonl`

- all operational provenance/source fields
- `pressure_mbar`
- `temperature_mdegc`

`log_battery_records.jsonl`

- all operational provenance/source fields
- `voltage_mv`
- `current_ua`

`log_unclassified_records.jsonl`

- all operational provenance/source fields
- `severity`
- `unclassified_reason`

### MER

Shared MER provenance fields:

- `instrument_id`
- `source_container`
- `source_file` (basename only in normalized JSONL outputs; full paths remain in manifest/run artifacts)

`mer_environment_records.jsonl`

- shared MER provenance fields
- `environment_kind`
- `gpsinfo_date`
- `raw_values`
- `line`

`mer_parameter_records.jsonl`

- shared MER provenance fields
- `parameter_kind`
- `raw_values`
- `line`

`mer_event_records.jsonl`

- shared MER provenance fields
- `block_index`
- `date`
- `pressure`
- `temperature`
- `criterion`
- `snr`
- `trig`
- `detrig`
- `fname`
- `smp_offset`
- `true_fs`
- `endianness`
- `bytes_per_sample`
- `sampling_rate`
- `stages`
- `normalized`
- `length`
- `data_payload_nbytes`
- `raw_info_line`
- `raw_format_line`

## Persisted Manifest And State Schemas

`manifests/latest.json`

- `run_id`
- `status`
- `started_at`
- `completed_at`
- `run_manifest`
- `outputs_manifest`
- `source_state_manifest`
- `preflight_status`

`manifests/runs/<run_id>/run.json`

- `run_id`
- `started_at`
- `completed_at`
- `input_root`
- `output_root`
- `normalization_version`
- `preflight_mode`
- `status`

`manifests/runs/<run_id>/outputs.json`

- `jsonl_outputs`
  - each row contains `path` and `size_bytes`
- `counts`
  - object keyed by JSONL basename without `.jsonl`

`manifests/runs/<run_id>/source_state.json`

- `input_root`
- `normalization_version`
- `raw_sources`
  - each row contains `source_file`, `source_kind`, `size_bytes`, and `content_hash`
- `decoder_state`
  - `null` when no BIN-dependent decoder state applies
  - otherwise contains `decoder_python`, `decoder_python_version`, `decoder_script`, `decoder_script_hash`, `preflight_mode`, `database_bundle_hash`, `database_files`, and `decoder_git_commit`

`manifests/runs/<run_id>/input_file_diffs.jsonl`

- one row per raw source file
- fields:
  - `source_file`
  - `source_kind`
  - `instrument_id`
  - `previous_exists`
  - `current_exists`
  - `previous_size_bytes`
  - `current_size_bytes`
  - `previous_hash`
  - `current_hash`
  - `change_kind`
  - `decoder_state_changed`
  - `run_id`

This file is strictly file-level. It does not store append/rewrite/noop decisions and does not contain standalone non-file invalidation rows.

`preflight_status.json`

- `requested_mode`
- `effective_mode`
- `database_update_attempted`
- `database_update_succeeded`
- `continued_after_failure`
- `failure_detail`
- `decoder_python`
- `decoder_script`
- `written_at`

`state/pruned_records.jsonl`

- `source_file`
- `source_kind`
- `instrument_id`
- `removed_at`

`removed_at` is the UTC timestamp when the pipeline detected and recorded the removal, not the underlying filesystem deletion time.

## Incremental Rerun Model

Incremental rerun decisions are per instrument and per family.

Behavior:

- append only when the only change is newly added raw source files
- rewrite when any previously seen raw source changes
- rewrite when any previously seen raw source is removed
- BIN-derived LOG outputs also rewrite when decoder state changes
- `noop` only when no relevant source or invalidation change is detected
- `--force-rewrite` overrides incremental planning and forces targeted instrument families to rewrite

Decoder-state invalidation:

- decoder-state changes invalidate BIN-derived outputs only
- LOG/MER-only floats must not be invalidated solely by decoder-state changes

JSONL safety model:

- outputs are source-ordered, not time-sorted
- no in-place mutation of existing JSONL lines
- safe modification paths are:
  - append new records
  - full rewrite of the affected family outputs

## Pruning Behavior

When a previously tracked raw source file is missing in a later stateful run:

- the affected family is rewritten from the remaining current sources
- records from the removed source disappear because they are not re-emitted
- the removal is recorded in `state/pruned_records.jsonl`

## Dry-Run Behavior

Dry-run behavior:

- compute the same rerun decisions as a real stateful run
- report per-instrument and per-family actions such as `append`, `rewrite`, and `noop`
- be completely side-effect free
- support both human-readable and JSON dry-run output
- dry-run must not write files of any kind, including manifests, status files, or reports in the output tree
