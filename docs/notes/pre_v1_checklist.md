# SPDX-License-Identifier: MIT

# Pre-v1.0.0 Checklist

This is the final hardening checklist before `v1.0.0`.
Check items off only when they are truly complete.
The goal is correctness, stability, and release clarity, not feature expansion.

## Public Contract Freeze

- [x] Explicitly confirm the installed CLI surface for `mermaid-records normalize`
- [x] Explicitly confirm the stateful vs stateless mode contract
- [x] Explicitly confirm the BIN decode preflight policy contract (`strict` vs `cached`, default behavior, and persistent `preflight_status.json` semantics)
- [x] Explicitly confirm the output directory layout contract
- [x] Explicitly confirm all JSONL output filenames
- [x] Explicitly confirm all JSONL record-family schemas
- [x] Explicitly confirm that JSONL outputs are source-ordered and not time-sorted
- [x] Explicitly confirm the manifest layout contract
- [x] Explicitly confirm the canonical `float_id` formatting/output contract
- [ ] Decide whether user-facing/package contract language should change from `float` to `instrument` (or another final term), and update consistently if so
- [x] Record any intentionally unsupported behaviors so they do not drift back in

## Schema Audit

- [x] Audit long-term stability of all LOG JSONL outputs
- [x] Audit long-term stability of all MER JSONL outputs
- [x] Audit `input_file_diffs.jsonl`
- [x] Audit `pruned_records.jsonl`
- [x] Confirm `pruned_records.jsonl` semantics are documented clearly (`removed_at` is detection time, not filesystem deletion time)
- [x] Audit `run.json`
- [x] Audit `outputs.json`
- [x] Audit `source_state.json`
- [x] Audit `preflight_status.json`
- [x] Confirm that all persisted schemas are documented at the level needed for release
- [ ] Confirm the documented MER field surface includes the observed Stanford variant fields

## Incremental And Rerun Correctness

- [x] Validate first stateful run into an empty output directory
- [x] Validate second stateful run with no raw-source changes
- [x] Validate append-only case with newly added LOG inputs
- [x] Validate append-only case with newly added MER inputs
- [x] Validate rewrite behavior for changed LOG input
- [x] Validate rewrite behavior for changed MER input
- [x] Validate rewrite and prune behavior for removed LOG input
- [x] Validate rewrite and prune behavior for removed MER input
- [x] Validate BIN decoder-state invalidation behavior
- [x] Validate stateless mode into a clean output directory
- [x] Validate stateless mode into an output directory containing manifests and confirm error behavior
- [x] Validate stateless mode with `--dry-run` and confirm it produces no manifests, no state files, and no output updates
- [x] Validate dry-run has zero filesystem side effects
- [x] Validate dry-run human-readable output
- [x] Validate dry-run JSON output
- [x] Validate first-run diff semantics treat prior state as empty (`previous_exists = false`, `previous_size_bytes = 0`, `previous_hash = null`)

## Packaging And Release Readiness

- [x] Sanity-check `pyproject.toml`
- [x] Sanity-check versioning and dynamic version source
- [x] Confirm README install and usage instructions are accurate
- [x] Confirm CLI help text is accurate and release-ready
- [x] Confirm root `LICENSE` file is present and correct
- [x] Confirm SPDX headers and package metadata remain consistent
- [x] Remove or update stale docs referencing removed behavior or old scope
- [ ] Confirm publish/release metadata is ready for `v1.0.0`

## Final Streamlining Pass

- [x] Remove dead or legacy internal clutter that should not ship into `v1.0.0`
- [x] Confirm no unnecessary modules remain in the package
- [x] Confirm no stale tests remain that preserve superseded behavior
- [x] Confirm no stale helper scripts remain that imply out-of-scope workflows
- [x] Confirm the package scope remains tightly limited to normalization only

## Must-Not-Forget

- [x] Rerun detection still considers both raw input diffs and decoder-state diffs
- [x] BIN-derived outputs are invalidated when decoder state changes
- [x] Raw server artifacts are still copied before processing because the external decoder may be destructive
- [x] Manifest/hash-based rerun logic remains in place for safe operation
