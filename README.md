[![Tests](https://github.com/Bathymetrix/mermaid-records/actions/workflows/ci.yml/badge.svg)](https://github.com/Bathymetrix/mermaid-records/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.14%2B-blue.svg)](https://pypi.org/project/mermaid-records/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)

# mermaid-records

`mermaid-records` normalizes raw MERMAID `BIN`, `LOG`, and `MER` artifacts into structured JSONL record families. It is a parsing and normalization layer only. It does not perform coordinate conversion, interval inference, waveform analysis, or higher-level interpretation.

## v1 contract

The release-facing v1 contract is intentionally narrow:

- supported raw inputs are `BIN`, `LOG`, and `MER`
- the installed CLI surface is `mermaid-records normalize`
- the package-root Python API is metadata-only (`mermaid_records.__version__`, authorship, and license metadata)
- normalized JSONL outputs are the primary supported output

`BIN` handling still depends on an external decoder step that produces `LOG` content. `mermaid-records` owns normalization around that seam; it does not reimplement the manufacturer decoder.

## Installation

```bash
python -m pip install .
```

For development:

```bash
python -m pip install -e .[dev]
```

## Canonical CLI example

Fixture-backed example; this command does not require the external BIN decoder:

```bash
mermaid-records normalize \
  --input-root data/fixtures/452.020-P-06 \
  --output-dir /tmp/mermaid-records-example
```

The normalize CLI supports:

- `--input-root` for `stateful mode`
- `--input-file` for `stateless mode`
- `--dry-run`
- `--json` for structured dry-run output only; it requires `--dry-run`
- `--verbose` / `-v`
- `--preflight-mode {strict,cached}` for BIN-backed runs

`--output-dir` may be omitted only when `MERMAID` is set. In that case, the CLI resolves the output root to `$MERMAID/records`.

## Execution modes

`mermaid-records normalize` has two execution modes:

- `stateful` mode is selected by `--input-root`
- `stateless` mode is selected by `--input-file`

`Stateful` mode persists `manifests/` and `state/` per instrument and enables incremental append/rewrite/noop planning. `Stateless` mode does not write manifests, does not use incremental state, and rewrites the targeted package-owned JSONL family outputs for each explicit run.

That `stateless` rewrite contract is intentional: rerunning the same explicit `--input-file` set does not silently duplicate JSONL rows.

When BIN decode preflight runs with a durable instrument output directory, the current run writes `preflight_status.json` at instrument root. This is tied to BIN decode, not to `stateful` mode by itself.

In `stateful` mode, `manifests/latest.json` includes `preflight_status` only when the current run produced that artifact. When no preflight runs, the field is absent rather than `null`, and stale preflight artifacts from earlier runs are not carried forward.

## Output layout

Typical per-instrument outputs look like:

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
    manifests/              # stateful mode only
    state/                  # stateful mode only
    preflight_status.json   # only when the current run's BIN decode preflight ran
```

Every per-instrument output directory materializes the canonical top-level JSONL file set even when some families are empty.

## Fixture coverage

The release-facing fixtures intentionally cover a few concrete float/data classes, not the full fleet:

- `452.020-P-06`: older-generation direct `LOG` + `MER` family with no `BIN` branch
- `465.152-R-0001`: compact real PSD / Stanford-style raw `BIN` + `MER` subset, including a metadata-only `MER` and an event-bearing `MER` with no `<FORMAT>` lines
- `467.174-T-0100`: BIN-backed family with tracked raw `BIN`, decoded `LOG`, raw `MER`, and `S61` fixture branches

These fixtures are representative test anchors for the implemented v1 behavior. They do not claim coverage for every float generation, record family variant, or decoder edge case.

## Python API posture

For v1, `mermaid_records` exposes only conservative package metadata at the package root. Functional helpers are intentionally not re-exported there as a broader stable API promise.

## Documentation map

- [docs/cli.md](docs/cli.md) — CLI flags, execution modes, and safe usage patterns
- [docs/limitations.md](docs/limitations.md) — preservation limits, mode-dependent artifacts, and allowed transformations
- [docs/ethos.md](docs/ethos.md) — scope discipline and design philosophy
- [docs/notes/normalization_release_contract.md](docs/notes/normalization_release_contract.md) — detailed behavioral reference

© 2026 Bathymetrix, LLC
