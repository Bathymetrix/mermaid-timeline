# mermaid-records

`mermaid-records` is a Python package for the normalization pipeline from raw MERMAID source artifacts to low-level JSONL record families. It is a canonical normalization layer, not an analysis layer.

Release-facing contract details, including persisted schema inventories, live in
`docs/notes/normalization_design_status.md`.

Current source inputs:

- `BIN`
- `LOG`
- `MER`

## Installation

Use Python 3.12 or newer.

Create a fresh local virtual environment from the repo root:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e '.[dev]'
```

After activation, the remainder of the README uses plain `python`, e.g.,

```sh
mermaid-records normalize -i /path/to/server --output-dir /path/to/output-dir
```

If the virtual environment is not activated, you can invoke the installed CLI directly as:

```sh
./.venv/bin/mermaid-records --help
```

## Package CLI

Show the installed CLI help:

```sh
mermaid-records --help
```

Run the normalization pipeline in stateful mode:

```sh
mermaid-records normalize --input-root /path/to/input-root --output-dir /path/to/output-dir
```

Run the normalization pipeline in stateless file-list mode (comma and/or space separated):

```sh
mermaid-records normalize --input-file /path/to/file1.LOG,/path/to/file2.MER /path/to/file3.LOG -o /path/to/output-dir
```

Run the normalization pipeline with `BIN` decode enabled:

```sh
mermaid-records normalize -i /path/to/input-root -o /path/to/output-dir --decoder-python /path/to/decoder-env/bin/python --decoder-script /path/to/automaid/scripts/preprocess.py
```

Preview the normalization plan without writing outputs, manifests, state files, or preflight status:

```sh
mermaid-records normalize -i /path/to/input-root -o /path/to/output-dir --dry-run
```

Force a full rewrite of targeted outputs instead of using incremental append/noop decisions:

```sh
mermaid-records normalize -i /path/to/input-root -o /path/to/output-dir --force-rewrite
```

Print the dry-run plan as JSON:

```sh
mermaid-records normalize -i /path/to/input-root -o /path/to/output-dir --dry-run --json
```

### CLI Resolution Order

The CLI keeps input selection explicit:

- `--input-root` selects stateful mode
- `--input-file` selects stateless mode

The CLI does not infer input paths from environment variables.

Output directory resolution order:

- `--output-dir` wins when provided
- otherwise, if `MERMAID` is set, the default output root is `$MERMAID/records`
- otherwise, the CLI errors and tells you to provide `--output-dir` or set `MERMAID`

Decoder Python resolution order:

- `--decoder-python` wins when provided
- otherwise `MERMAID_RECORDS_DECODER_PYTHON` is used when set
- otherwise the CLI errors only if the selected run actually contains `BIN` inputs

Decoder script resolution order:

- `--decoder-script` wins when provided
- otherwise `MERMAID_RECORDS_DECODER_SCRIPT` is used when set
- otherwise the CLI errors only if the selected run actually contains `BIN` inputs

Preflight mode defaults to:

- `strict`

Configured-machine example:

```sh
export MERMAID=/path/to/mermaid
export MERMAID_RECORDS_DECODER_PYTHON=/path/to/decoder-env/bin/python
export MERMAID_RECORDS_DECODER_SCRIPT=/path/to/automaid/scripts/preprocess.py
mermaid-records normalize -i /path/to/server
```

On a configured machine like this, the CLI uses:

- output dir: `$MERMAID/records`
- decoder python: `$MERMAID_RECORDS_DECODER_PYTHON`
- decoder script: `$MERMAID_RECORDS_DECODER_SCRIPT`

Explicit CLI arguments always override environment variables.

The installed public CLI surface is intentionally small:

- `mermaid-records normalize`

The `normalize` command supports exactly two execution modes:

- stateful mode
  - triggered by `--input-root`
  - writes manifests
  - enables incremental rerun detection and pruning
  - errors on `BIN` input unless decoder paths are supplied
- stateless mode
  - triggered by `--input-file`
  - ignores manifests and writes none
  - performs no pruning
  - errors if the target output tree already contains any `manifests/`

The `normalize` command writes:

- one subdirectory per instrument under the output root
- in stateful corpus mode, full serial-number subdirectory names derived from `<serial>.vit` files in `--input-root`, for example `467.174-T-0100/`
- per-instrument LOG JSONL outputs:
  - `log_operational_records.jsonl`
  - `log_acquisition_records.jsonl`
  - `log_ascent_request_records.jsonl`
  - `log_gps_records.jsonl`
  - `log_parameter_records.jsonl`
    - grouped startup/dive-parameter episodes preserved from LOG continuation lines
  - `log_testmode_records.jsonl`
    - grouped test-mode sessions preserved from LOGs
  - `log_sbe_records.jsonl`
    - grouped SBE/profil operational episodes preserved from LOGs
  - `log_transmission_records.jsonl`
  - `log_measurement_records.jsonl`
  - `log_unclassified_records.jsonl`
- per-instrument MER JSONL outputs:
  - `mer_environment_records.jsonl`
  - `mer_parameter_records.jsonl`
  - `mer_event_records.jsonl`
- per-instrument `manifests/` in stateful mode
- per-run `manifests/runs/<run_id>/input_file_diffs.jsonl` in stateful mode
- per-instrument `state/` for pruning records in stateful mode
- per-instrument `preflight_status.json` when BIN decode preflight runs and a durable output directory is in use

Invariant details:

- JSONL outputs are ordered by deterministic source processing order, not time order
- JSONL field ordering is explicit and stable: provenance/identity first, then time, then family metadata, then payload/accounting, then raw fallback fields
- existing JSONL lines are never mutated in place; the safe update paths are append and full rewrite
- `--force-rewrite` overrides incremental planning and forces targeted instrument families to rewrite
- dry-run reuses the same planning and diff logic as a real run but performs zero filesystem writes
- canonical `instrument_id` is resolved from `src/mermaid_records/parse_instrument_name.py` when a full serial is available, for example `452.020-P-08 -> P0008` and `467.174-T-0100 -> T0100`

## Decoder Requirements

`BIN -> LOG` decode uses external automaid/preprocess code through a subprocess wrapper in `mermaid_records.bin2log`.

The package does not auto-detect a conda environment name. Callers must supply decoder paths either explicitly on the CLI or through environment variables:

- the path to the Python executable for the automaid decoder environment
- the path to `preprocess.py`

In practice this looks like:

```sh
--decoder-python /path/to/conda/env/bin/python
--decoder-script /path/to/automaid/scripts/preprocess.py
```

or:

```sh
export MERMAID_RECORDS_DECODER_PYTHON=/path/to/conda/env/bin/python
export MERMAID_RECORDS_DECODER_SCRIPT=/path/to/automaid/scripts/preprocess.py
```

If automaid expects environment variables such as `MERMAID`, set them before running the decode-enabled scripts.

BIN decode preflight supports exactly two modes:

- `strict`
  - default
  - requires successful live `database_update(...)`
- `cached`
  - still attempts `database_update(...)`
  - if refresh fails, emits a warning and continues using cached local decoder state

When BIN decode preflight runs with a durable output directory, the wrapper also writes
`preflight_status.json` there so non-interactive runs can audit whether refresh succeeded,
failed closed, or continued in cached-degraded mode.

Unsupported behavior that is intentionally out of scope for this package:

- analysis or timeline interpretation
- unit or coordinate conversions
- inferred acquisition windows from assertion lines or sample metadata
- DET / REQ logic
- `CYCLE` / `CYCLE.h`
- any workflow that mutates raw source files in place

## Internal Dev Utilities

The installed and supported workflow is `mermaid-records normalize`.

The repo still includes a small number of internal developer utilities for decoder
adapter work on tracked fixtures. These are not installed package entry points and
should not be treated as parallel normalization workflows.

### Audit Normalize CLI Combinations

Use the matrix harness when you want to exercise `normalize` across output
resolution, decoder resolution, preflight mode, `--dry-run`, `--force-rewrite`,
`--json`, and `--verbose`, while continuing past failures and logging every run.

The script writes:

- one JSONL row per planned run to `audit_normalize_cli/reports/results.jsonl`
- per-run `stdout.txt`, `stderr.txt`, `command.txt`, and `spec.json`
- aggregate `summary.json` and `summary.md`
- `inputs/all_raw_files.txt` so the exact stateless file list is captured

Repo-local example:

```sh
python scripts/audit_normalize_cli_matrix.py \
  --input-root ~/mermaid/server_everyone \
  --output-root ~/Desktop/records_test \
  --cli-command "mermaid-records"
```

If your machine exposes a wrapper command named `mermaid`, point the harness at it:

```sh
python scripts/audit_normalize_cli_matrix.py \
  --input-root ~/mermaid/server_everyone \
  --output-root ~/Desktop/records_test \
  --cli-command "mermaid"
```

For BIN-containing corpora, provide valid decoder paths if you want the successful
BIN combinations to run instead of being logged as skipped/unavailable:

```sh
python scripts/audit_normalize_cli_matrix.py \
  --input-root ~/mermaid/server_everyone \
  --output-root ~/Desktop/records_test \
  --cli-command "mermaid-records" \
  --decoder-python /path/to/decoder-env/bin/python \
  --decoder-script /path/to/automaid/scripts/preprocess.py
```

### Profile The Wrapped BIN To LOG Decode Path

Profile the current batch decode workflow on the newer-generation fixture family:

```sh
MERMAID=/path/to/mermaid python scripts/profile_bin2log_fixture.py 1 --decoder-python /path/to/decoder-env/bin/python --decoder-script /path/to/automaid/scripts/preprocess.py
```

```sh
MERMAID=/path/to/mermaid python scripts/profile_bin2log_fixture.py 5 --decoder-python /path/to/decoder-env/bin/python --decoder-script /path/to/automaid/scripts/preprocess.py
```

```sh
MERMAID=/path/to/mermaid python scripts/profile_bin2log_fixture.py 20 --decoder-python /path/to/decoder-env/bin/python --decoder-script /path/to/automaid/scripts/preprocess.py
```

This prints one JSON summary per run with phase timings for the current wrapped decode batch path.

### Materialize BIN Fixture LOGs

Decode BIN fixtures in a temporary workspace and compare the emitted LOGs against
tracked LOG fixtures:

```sh
MERMAID=/path/to/mermaid python scripts/materialize_bin_logs.py --decoder-python /path/to/decoder-env/bin/python --decoder-script /path/to/automaid/scripts/preprocess.py
```

This is an internal fixture-maintenance utility around the external decoder adapter.
