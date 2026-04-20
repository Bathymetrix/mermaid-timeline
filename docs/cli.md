# CLI

## Entry point

The installed command is:

```bash
mermaid-records normalize
```

The normalize subcommand does **not** use positional `<input> <output>` arguments.

## Inputs and outputs

Normalization is driven through explicit flags.

Typical forms are:

```bash
mermaid-records normalize --input-root <INPUT_ROOT> --output-dir <OUTPUT_DIR>
```

or, for targeted runs:

```bash
mermaid-records normalize --input-file <INPUT_FILE> --output-dir <OUTPUT_DIR>
```

Supported normalize flags currently are:

- `--input-root`
- `--input-file`
- `--output-dir`
- `--decoder-python`
- `--decoder-script`
- `--preflight-mode {strict,cached}`
- `--dry-run`
- `--force-rewrite`
- `--json` (requires `--dry-run`)
- `--verbose` / `-v`

`--output-dir` is optional only when the `MERMAID` environment variable is set, in which case the CLI resolves the output directory to `$MERMAID/records`.

`--json` is valid only with `--dry-run`. The CLI exits early with `--json requires --dry-run` for any other combination.

## Execution modes

The CLI has two important behavioral modes:

- **stateful mode** — selected by `--input-root`; writes normalization bookkeeping and manifests
- **stateless mode** — selected by `--input-file`; performs normalization without manifest persistence

This distinction matters for:

- whether `manifests/` and `state/` exist
- whether `preflight_status.json` can exist
- whether malformed/non-normalizable content is persisted in manifest artifacts
- how rewrite/bookkeeping behavior should be interpreted

If you are documenting or debugging normalization results, always keep the selected execution mode in mind.

Stateless mode is rewrite-only in v1 for the targeted instrument outputs. Because stateless runs do not persist manifests or incremental state, rerunning the same explicit inputs rewrites the package-owned JSONL families instead of appending to them.

## Output families

Typical per-instrument outputs include LOG and MER JSONL families such as:

- `log_acquisition_records.jsonl`
- `log_ascent_request_records.jsonl`
- `log_battery_records.jsonl`
- `log_gps_records.jsonl`
- `log_operational_records.jsonl`
- `log_parameter_records.jsonl`
- `log_pressure_temperature_records.jsonl`
- `log_sbe_records.jsonl`
- `log_testmode_records.jsonl`
- `log_transmission_records.jsonl`
- `log_unclassified_records.jsonl`
- `mer_environment_records.jsonl`
- `mer_event_records.jsonl`
- `mer_parameter_records.jsonl`

Stateful runs materialize:

- `manifests/`
- `state/`

Additionally:

- `preflight_status.json` is written at instrument root only when BIN decode preflight runs with a durable output directory
- stateless runs do not write `manifests/`, `state/`, or `preflight_status.json`

## Force rewrite

`--force-rewrite` is a targeted regeneration mechanism.

For the targeted instrument output directories, it removes package-owned generated artifacts before regeneration so that stale outputs from older layouts do not persist. It should be understood as instrument-scoped cleanup and rebuild, not global deletion of all outputs under the entire output root.

In stateless mode, `--force-rewrite` does not change the rerun contract: targeted instrument outputs already rewrite by default, and the flag still does not enable manifests or other stateful bookkeeping.

## Operational guidance

For authoritative behavioral details, including mode semantics and contract-level guarantees, pair this document with:

- `docs/limitations.md`
- `docs/notes/normalization_release_contract.md`
