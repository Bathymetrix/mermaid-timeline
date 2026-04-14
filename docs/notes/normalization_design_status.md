# SPDX-License-Identifier: MIT

# Normalization Design Status

## Current Pipeline Architecture

`mermaid-records` currently centers on a normalization pipeline from raw `BIN`, `LOG`, and `MER` inputs to low-level JSONL record families.

`CYCLE` and `CYCLE.h` are out of scope for this package.

Execution modes:

- `stateful`
  - input is a directory root
  - manifests are read and written
  - incremental rerun logic is enabled
- `stateless`
  - input is an explicit file list
  - manifests are ignored and not written
  - no incremental logic
  - no pruning
  - must not target an output tree that already contains manifests

In stateful corpus mode, per-float output directories are organized under the output root and should use the full float serial from `<serial>.vit` files in the input root when available.

## Manifest Structure

Per float:

```text
<output_root>/<float-serial>/
  manifests/
    latest.json
    runs/
      <run_id>/
        run.json
        outputs.json
        source_state.json
        input_file_diffs.jsonl
        preflight_status.json
```

Notes:

- `latest.json` points to the most recent run for that float.
- `run.json` stores run metadata and status.
- `outputs.json` stores output inventory and row counts.
- `source_state.json` stores raw source identity and decoder-state identity.
- `input_file_diffs.jsonl` stores one row per raw source file with file-level diff fields only.
- `preflight_status.json` is present when BIN preflight ran.

## Incremental Rerun Model

Incremental rerun decisions are per float and per family.

Current behavior:

- append only when the only change is newly added raw source files
- rewrite when any previously seen raw source changes
- rewrite when any previously seen raw source is removed
- `noop` only when no relevant source or invalidation change is detected

Decoder-state invalidation:

- decoder-state changes invalidate BIN-derived outputs only
- LOG/MER-only floats must not be invalidated solely by decoder-state changes

Current JSONL safety model:

- outputs are source-ordered, not time-sorted
- no in-place mutation of existing JSONL lines
- safe modification paths are:
  - append new records
  - full rewrite of the affected family outputs

## Pruning Behavior

When a previously tracked raw source file is missing in a later stateful run:

- the affected family is rewritten from the remaining current sources
- records from the removed source disappear because they are not re-emitted
- the removal is recorded in:

```text
<output_root>/<float-serial>/state/pruned_records.jsonl
```

Each prune row records:

- `source_file`
- `source_kind`
- `float_id`
- `removed_at`

`removed_at` is the UTC timestamp when the pipeline observed and recorded the removal, not the filesystem deletion time.

## Dry-Run Behavior (Planned)

Planned dry-run/report behavior should:

- compute the same rerun decisions as a real stateful run
- report per-float and per-family actions such as `append`, `rewrite`, and `noop`
- be completely side-effect free
- support both human-readable and JSON dry-run output

Constraint:

- dry-run must not write files of any kind, including manifests, status files, or reports in the output tree

## Remaining Steps Before v1.0.0

- implement dry-run/report behavior
- do the final package-streamlining pass to confirm each public surface is necessary for the core normalization package
