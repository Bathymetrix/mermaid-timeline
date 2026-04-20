# CLI

## Entry point

The installed command is:

```bash
mermaid-records normalize
```

The `normalize` subcommand does not accept positional input or output paths. Mode selection and I/O are controlled by flags.

## Required mode selector

Exactly one of these must be supplied:

- `-i`, `--input-root <PATH>`
  - selects `stateful` mode
  - recursively discovers supported raw inputs under the root
- `--input-file <PATH ...>`
  - selects `stateless` mode
  - accepts repeated uses plus comma-separated or space-separated file lists
  - normalizes only the explicitly listed raw files

Supported raw input types in v1 are `BIN`, `LOG`, and `MER`.

## Output resolution

- `-o`, `--output-dir <PATH>`
  - explicit output root for normalized records
- when `--output-dir` is omitted and `MERMAID` is set
  - the CLI uses `$MERMAID/records`
- when neither is available
  - the CLI exits with an error

## Decoder and preflight flags

- `--decoder-python <PATH>`
  - Python executable for the external decoder environment
- `--decoder-script <PATH>`
  - path to the external `preprocess.py`
- `--preflight-mode {strict,cached}`
  - applies only when BIN inputs are present and decoder-backed BIN decode preflight runs
  - `strict`: live decoder database refresh failure is terminal
  - `cached`: live refresh is still attempted, but explicit degraded continuation is allowed after failure

`--decoder-python` and `--decoder-script` must be provided together, either directly or through `MERMAID_RECORDS_DECODER_PYTHON` and `MERMAID_RECORDS_DECODER_SCRIPT`.

## Planning and reporting flags

- `--dry-run`
  - computes the normalization plan and file-level diffs without writing files
- `--json`
  - prints structured dry-run output instead of the human-readable summary
  - dry-run only
  - the CLI rejects `--json` without `--dry-run` with `--json requires --dry-run`
- `--force-rewrite`
  - forces targeted family rewrites instead of append/noop incremental decisions
  - removes package-owned generated artifacts for each targeted instrument before regeneration
- `-v`, `--verbose`
  - prints an expanded end-of-run summary
  - does not change normalization behavior

## Execution modes

### Stateful mode

Stateful mode is selected by `--input-root`.

Behavior:

- persists `manifests/` and `state/` under each targeted instrument directory
- enables incremental append/rewrite/noop planning
- records file-level diffs in `manifests/runs/<run_id>/input_file_diffs.jsonl`
- records malformed/skipped-source recovery artifacts in per-run manifest files
- records removed-source observations in `state/pruned_records.jsonl`
- creates a fresh manifest `run_id` for every executed run, even on immediate reruns

Stateful mode is the only mode that uses persisted manifests to decide later append, rewrite, noop, and pruning behavior.

### Stateless mode

Stateless mode is selected by `--input-file`.

Behavior:

- does not write `manifests/`
- does not write `state/`
- does not use persisted incremental state
- rejects any target output tree that already contains `manifests/`
- rewrites the targeted package-owned JSONL family outputs on every real run

That rewrite-only `stateless` contract is the safety mechanism that prevents silent duplication on reruns. If you rerun the same explicit file list, the targeted family outputs are regenerated rather than appended to.

`--force-rewrite` does not change the fundamental stateless contract. It still does not enable manifests or incremental state; it only keeps cleanup behavior explicit for targeted package-owned outputs.

## Manifest and preflight artifacts

- `manifests/` and `state/` are stateful-only
- `preflight_status.json` is written only when the current run performs BIN decode preflight with a durable instrument output directory
- `preflight_status.json` can therefore appear in either execution mode if the run actually performs BIN-backed normalization with decoder preflight
- in `stateful` mode, `manifests/latest.json` includes `preflight_status` only when the current run produced that artifact
- when no preflight runs, `manifests/latest.json` omits `preflight_status` rather than storing `null`
- stale preflight artifacts from earlier runs are cleared before a new run is recorded
- dry-run writes none of these artifacts in either mode

## Safe stateless example

Fixture-backed example; decoder-free and safe to rerun because stateless mode rewrites the targeted output families:

```bash
mermaid-records normalize \
  --input-file \
    data/fixtures/452.020-P-06/log/06_67C95E46.LOG \
    data/fixtures/452.020-P-06/mer/06_67C95E38.MER \
  --output-dir /tmp/mermaid-records-stateless
```

On repeated runs against the same `/tmp/mermaid-records-stateless` target:

- the package-owned LOG and MER family outputs for instrument `P0006` are rewritten
- rows from prior stateless runs are not silently duplicated
- unknown non-package files in the instrument directory are preserved

## Related references

- [README.md](../README.md) — release overview and canonical stateful example
- [docs/limitations.md](limitations.md) — preservation limits and allowed transformations
- [docs/notes/normalization_release_contract.md](notes/normalization_release_contract.md) — detailed behavioral contract
