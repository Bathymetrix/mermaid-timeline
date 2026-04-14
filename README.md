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
mermaid-records normalize -i ~/mermaid/server_everyone  --decoder-python  ~/miniconda3/envs/pymaid3.10/bin/python --decoder-script $AUTOMAID/scripts/preprocess.py -o ~/mermaid/records
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
MERMAID=/path/to/mermaid mermaid-records normalize -i /path/to/input-root -o /path/to/output-dir --decoder-python /path/to/decoder-env/bin/python --decoder-script /path/to/automaid/scripts/preprocess.py
```

Preview the normalization plan without writing outputs, manifests, state files, or preflight status:

```sh
mermaid-records normalize -i /path/to/input-root -o /path/to/output-dir --dry-run
```

Print the dry-run plan as JSON:

```sh
mermaid-records normalize -i /path/to/input-root -o /path/to/output-dir --dry-run --json
```

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

- one subdirectory per float under the output root
- in stateful corpus mode, full serial-number subdirectory names derived from `<serial>.vit` files in `--input-root`, for example `467.174-T-0100/`
- per-float LOG JSONL outputs:
  - `log_operational_records.jsonl`
  - `log_acquisition_records.jsonl`
  - `log_ascent_request_records.jsonl`
  - `log_gps_records.jsonl`
  - `log_transmission_records.jsonl`
  - `log_measurement_records.jsonl`
  - `log_unclassified_records.jsonl`
- per-float MER JSONL outputs:
  - `mer_environment_records.jsonl`
  - `mer_parameter_records.jsonl`
  - `mer_data_records.jsonl`
- per-float `manifests/` in stateful mode
- per-run `manifests/runs/<run_id>/input_file_diffs.jsonl` in stateful mode
- per-float `state/` for pruning records in stateful mode
- per-float `preflight_status.json` when BIN decode preflight runs and a durable output directory is in use

Invariant details:

- JSONL outputs are ordered by deterministic source processing order, not time order
- existing JSONL lines are never mutated in place; the safe update paths are append and full rewrite
- dry-run reuses the same planning and diff logic as a real run but performs zero filesystem writes
- canonical `float_id` is resolved from `src/mermaid_records/parse_float_name.py` when a full serial is available, for example `452.020-P-08 -> P0008` and `467.174-T-0100 -> T0100`

## Decoder Requirements

`BIN -> LOG` decode uses external automaid/preprocess code through a subprocess wrapper in `mermaid_records.bin2log`.

The package does not auto-detect a conda environment name. Callers must supply:

- the path to the Python executable for the automaid decoder environment
- the path to `preprocess.py`

In practice this looks like:

```sh
--decoder-python /path/to/conda/env/bin/python
--decoder-script /path/to/automaid/scripts/preprocess.py
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

## Helper Scripts

These scripts are repo helper entry points rather than installed package subcommands.

### Profile The Full Normalization Pipeline

Profile one fixture root from raw source artifacts through JSONL outputs:

Older-generation canonical fixture root:

```sh
python scripts/profile_normalization_pipeline.py data/fixtures/452.020-P-06
```

Newer-generation canonical fixture root with `BIN` decode enabled:

```sh
MERMAID=/path/to/mermaid python scripts/profile_normalization_pipeline.py data/fixtures/467.174-T-0100 --decoder-python /path/to/decoder-env/bin/python --decoder-script /path/to/automaid/scripts/preprocess.py
```

This prints one flat JSON object with counts and timings for:

- input discovery
- `BIN -> LOG` decode
- `LOG` parsing and normalization
- `MER` parsing and normalization
- JSONL writing

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

### Generate LOG JSONL Prototypes

Generate the current LOG-derived JSONL families from the representative LOG subset:

```sh
python scripts/generate_log_jsonl_prototype.py
```

Outputs are written under:

```sh
data/fixtures/log_examples_representative_06_0100/jsonl_prototype/
```

Current LOG JSONL outputs are:

- `log_operational_records.jsonl`
- `log_acquisition_records.jsonl`
- `log_ascent_request_records.jsonl`
- `log_gps_records.jsonl`
- `log_transmission_records.jsonl`
- `log_measurement_records.jsonl`
- `log_unclassified_records.jsonl`

### Generate MER JSONL Prototypes

Generate the current MER-derived JSONL families from the representative MER subset:

```sh
python scripts/generate_mer_jsonl_prototype.py
```

Outputs are written under:

```sh
data/fixtures/mer_examples_representative_06_0100/jsonl_prototype/
```

Current MER JSONL outputs are:

- `mer_environment_records.jsonl`
- `mer_parameter_records.jsonl`
- `mer_data_records.jsonl`

### Other Dev Scripts

Additional helper scripts currently in the repo:

- `scripts/materialize_bin_logs.py`

These are dev-facing workflow utilities around the current external decoder adapters.
