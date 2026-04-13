# mermaid-records

`mermaid-records` is a Python package for the normalization pipeline from raw MERMAID source artifacts to low-level JSONL record families.

Current source inputs:

- `BIN`
- `LOG`
- `MER`

The package keeps decode, parsing, normalization, and later interpretation separate. It is a canonical normalization layer, not an analysis layer.

## Installation

```bash
pip install -e .[dev]
```

## Package CLI

Show the installed CLI help:

```bash
mermaid-records --help
```

Inspect a raw `MER` file:

```bash
mermaid-records inspect-mer /path/to/file.MER
```

Inspect a parsed operational `LOG` file:

```bash
mermaid-records inspect-cycle /path/to/file.LOG
```

`inspect-mer` prints one line per parsed MER event block with:

- block `date` when present
- literal `EVENT`
- payload byte count

`inspect-cycle` is the current operational-text inspection command name. It prints one line per parsed operational record with:

- parsed canonical timestamp
- source kind
- subsystem and code
- message text

Even though the command name is still `inspect-cycle`, use it for `LOG` inspection in the current README examples.

## Decoder Requirements

`BIN -> LOG` decode uses external automaid/preprocess code through a subprocess wrapper in `mermaid_records.bin2log`.

The package does not auto-detect a conda environment name. Callers must supply:

- the path to the Python executable for the automaid decoder environment
- the path to `preprocess.py`

In practice this looks like:

```bash
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

## Helper Scripts

These scripts are repo helper entry points rather than installed package subcommands.

### Profile The Full Normalization Pipeline

Profile one fixture root from raw source artifacts through JSONL outputs:

Older-generation canonical fixture root:

```bash
./.venv/bin/python scripts/profile_normalization_pipeline.py data/fixtures/452.020-P-06
```

Newer-generation canonical fixture root with `BIN` decode enabled:

```bash
MERMAID=/Users/jdsimon/mermaid ./.venv/bin/python scripts/profile_normalization_pipeline.py data/fixtures/467.174-T-0100 --decoder-python /Users/jdsimon/miniconda3/envs/pymaid3.10/bin/python --decoder-script /Users/jdsimon/programs/automaid/scripts/preprocess.py
```

This prints one flat JSON object with counts and timings for:

- input discovery
- `BIN -> LOG` decode
- `LOG` parsing and normalization
- `MER` parsing and normalization
- JSONL writing

### Profile The Wrapped BIN To LOG Decode Path

Profile the current batch decode workflow on the newer-generation fixture family:

```bash
MERMAID=/Users/jdsimon/mermaid ./.venv/bin/python scripts/profile_bin2log_fixture.py 1 --decoder-python /Users/jdsimon/miniconda3/envs/pymaid3.10/bin/python --decoder-script /Users/jdsimon/programs/automaid/scripts/preprocess.py
```

```bash
MERMAID=/Users/jdsimon/mermaid ./.venv/bin/python scripts/profile_bin2log_fixture.py 5 --decoder-python /Users/jdsimon/miniconda3/envs/pymaid3.10/bin/python --decoder-script /Users/jdsimon/programs/automaid/scripts/preprocess.py
```

```bash
MERMAID=/Users/jdsimon/mermaid ./.venv/bin/python scripts/profile_bin2log_fixture.py 20 --decoder-python /Users/jdsimon/miniconda3/envs/pymaid3.10/bin/python --decoder-script /Users/jdsimon/programs/automaid/scripts/preprocess.py
```

This prints one JSON summary per run with phase timings for the current wrapped decode batch path.

### Generate LOG JSONL Prototypes

Generate the current LOG-derived JSONL families from the representative LOG subset:

```bash
./.venv/bin/python scripts/generate_log_jsonl_prototype.py
```

Outputs are written under:

```bash
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

```bash
./.venv/bin/python scripts/generate_mer_jsonl_prototype.py
```

Outputs are written under:

```bash
data/fixtures/mer_examples_representative_06_0100/jsonl_prototype/
```

Current MER JSONL outputs are:

- `mer_environment_records.jsonl`
- `mer_parameter_records.jsonl`
- `mer_data_records.jsonl`

### Other Dev Scripts

Additional helper scripts currently in the repo:

- `scripts/materialize_bin_logs.py`
- `scripts/validate_bin2cycle.py`
- `scripts/audit_bin2cycle_corpus.py`

These are dev-facing workflow utilities around the current external decoder adapters.
