# AGENTS

## Persistent Rules

- Never mix parsing and interpretation.
- Keep record interfaces stable.
- Always preserve unknown MER record types.
- Prefer generators for file parsing where practical.
- Add or update tests with any real logic change.
- Prefer small, coherent commits and always tell the user when the current state is a good time to commit.
- Any time suggesting a commit, also suggest a matching commit comment/message.
- Treat this file as the persistent handoff document for future Codex sessions and update it whenever assumptions, boundaries, fixtures, or workflow rules change.
- As the package nears completion, explicitly remind the user to revisit the pending BIN decode policy discussion: whether full-package decode workflows should require live `database_update(...)` or support an offline/cached mode.
- Before publishing v1.0.0, do a final package-streamlining pass and ask of each public module/surface: is this necessary or useful in the core package? Borderline derived-product helpers such as coverage/interval-style modules may belong in a separate package if they do not support the streamlined parsing/normalization core.
- Add the exact Bathymetrix header only to `src/mermaid_records/__init__.py` and `src/mermaid_records/cli.py`.
- Do not add the Bathymetrix header to `README.md`, tests, or internal implementation modules unless the user explicitly asks.
- When referring to an output cycle file, call it `CYCLE` (parallel to `BIN`, `LOG`, and `MER`).
- Never delete source raw files from their original path. This includes `.BIN`, `.MER`, and future raw formats such as `.vit`. If an external decoder is destructive, it must run only on copied files in a temporary workspace.
- In the corpus, raw input data live in `server`, and manufacturer-produced output used for audit/reference comparison lives in `processed`.
- Before writing a new audit workflow, first scan existing audit code for reusable logic and easy refactors instead of starting from a fresh script by default.
- Keep code DRY. Check often for shared logic that should be refactored into reusable helpers, but verify behavior before and after any DRY-driven refactor instead of assuming equivalence.
- When the user's intent is clear, rewrite their rules into cleaner internal guidance here rather than copying rough wording verbatim. If the intent is unclear, ask before recording the rule.
- Always push back, disagree, or suggest a better alternative when warranted. Do not agree reflexively. When suggesting a different path, give the reason plainly and concretely.
- Package license defaults to MIT. A root `LICENSE` file must exist.
- Every Python source file must include `SPDX-License-Identifier: MIT`, but must not include the full license text.
- The exact Bathymetrix header belongs only in `src/mermaid_records/__init__.py` and `src/mermaid_records/cli.py`, unless the user explicitly expands that set.
- Use `Bathymetrix™` only in approved file headers and rare top-level visible branding. Do not use the trademark marker in identifiers, module names, imports, docstrings, or inline comments.
- Package authorship/licensing metadata must live in exactly one location: `src/mermaid_records/__init__.py` via `__author__`, `__license__`, and `__copyright__`.

## Versioning Rule — Single Source of Truth

- The canonical version must live in `src/mermaid_records/__init__.py` as `__version__`.
- `pyproject.toml` must use dynamic versioning via `[tool.setuptools.dynamic]` with `version = {attr = "mermaid_records.__version__"}`.
- No other hardcoded package version strings are allowed in the repo.

## Project Purpose

`mermaid-records` is a Python package to normalize `LOG`/`BIN` and `MER` into parseable record families.

Supported inputs currently include:

- `.BIN` files
- `.LOG` files
- `.CYCLE` text files
- `.CYCLE.h` text files
- `.MER` files

This layer is strictly for parsing and structured extraction.

Do not add:

- unit conversions
- coordinate conversions
- inferred durations from sample counts and rates
- acquisition inference beyond explicit `acq started` / `acq stopped`
- DET / REQ logic
- higher-level timeline interpretation

At this stage the package is primarily converting manufacturer formats into machine-parseable normalized record families while preserving provenance.

## Current Parsing Scope

Canonical long-term source model:

- upstream raw `.BIN`
- upstream raw `.LOG`
- upstream raw `.MER`

Derived operational products:

- emitted raw `.CYCLE`
- processed `.CYCLE.h`

Processed `.CYCLE` and `.CYCLE.h` remain supported as compatibility, reference, and comparison paths rather than canonical long-term primitives when raw `LOG` exists.

Do not frame `LOG`/`BIN` as purely operational and `MER` as purely data. These containers can each carry mixed operational, location, transmission, and accounting signals. Distinguish them by source container and provenance, not by assuming clean semantic separation.

Decode/parsing boundary:

- decode: raw `BIN` -> decoded `LOG`, with optional later grouping to emitted raw `CYCLE`
- parsing: consume raw `LOG`, emitted raw `CYCLE`, or processed `.CYCLE.h`
- interpretation/timeline logic remains separate and should not be mixed into either layer

For v1 normalization work, prefer the `BIN` -> `LOG` boundary as the primary decode seam. Treat `CYCLE` as the later derived grouping step built from decoded LOG content.

Mirror the upstream preprocess call order responsibly:

- `database_update(...)` is a batch preflight step
- `concatenate_files(...)` and `decrypt_all(...)` are part of the per-workspace `BIN` -> `LOG` decode path
- `convert_in_cycle(...)` is the later derived `LOG` -> `CYCLE` step
- `concatenate_rbr_files(...)` may also be part of preprocessing, but should not force interpretation into the decode layer

Do not call `database_update(...)` once per `BIN`; prefer a single explicit refresh before a batch decode workflow.
Preflight should fail closed. If `database_update(...)` or any other preprocess preflight step reports an error, stop the workflow instead of continuing with stale or partial state.

### Operational Text Sources

Use one common `OperationalLogEntry` model for `LOG`, `CYCLE`, and `.CYCLE.h` with:

- `time`
- `subsystem`
- `code`
- `message`
- `source_kind`
- `raw_line`
- `source_file`

Preserve source identity via `source_kind`:

- `log`
- `cycle`
- `cycle_h`

Do not collapse `LOG`, `CYCLE`, and `.CYCLE.h` into one canonical source during parsing. Preserve provenance even when their content overlaps.

Normalized record-family direction to keep in mind during cleanup and naming:

- `operational_records`
- `location_records`
- `transmission_records`
- `acquisition_records`
- `ascent_request_records`
- `mer_data_blocks`
- `acquisition_intervals`
- `measurement_records`
- `gps_records`
- `unclassified_operational_records`

Do not fully implement these families unless the current code naturally supports them, but prefer names and module roles that leave room for this direction.

For derived operational-family prototypes, no parsed `OperationalLogEntry` should disappear silently. Each parsed operational line must end up either in one or more derived family streams or in `unclassified_operational_records`.

For acquisition evidence prototypes, preserve the distinction between exact transitions and state assertions:

- `acq started` / `acq stopped` are transition evidence
- `acq already started` / `acq already stopped` are assertion evidence

Do not infer intervals from assertion lines in the normalization layer.

For ascent-request prototypes, classify only explicit request outcomes such as `ascent request accepted` and `ascent request rejected`. Do not infer ascent-request state from other ascent-related lines.

For GPS prototypes, emit one record per clearly GPS-related LOG line such as `GPS fix...`, raw latitude/longitude lines, `hdop`/`vdop`, `GPSACK`, and `GPSOFF`. Do not group lines into fixes or compute derived position/timing values in the normalization layer.

For generated JSONL filenames, prefix low-level LOG-derived outputs with `log_` and reserve analogous `mer_` prefixes for future MER-derived JSONL outputs. Do not treat this as a package/module naming rule.

Use `cycle` in names only when referring to the concrete derived artifact types `CYCLE` or `.CYCLE.h`. Shared parser and normalization surfaces should prefer `operational` naming instead.

Acquisition windows may be extracted only from explicit:

- `acq started`
- `acq stopped`

Ignore lines like:

- `acq already started`
- `acq already stopped`

### `.MER`

Parse `.MER` files into:

- one `MerFileMetadata`
- zero or more `MerDataBlock` values

For metadata:

- preserve raw `ENVIRONMENT` lines
- preserve raw `PARAMETERS` lines
- extract repeated `GPSINFO`, `DRIFT`, and `CLOCK` structures conservatively
- keep GPS coordinates in raw string form

For event blocks:

- parse `INFO`
- parse `FORMAT`
- preserve `DATA` payload bytes without waveform interpretation

## Current File/Layout Assumptions

- Use one common operational-line parser/model across `LOG`, `CYCLE`, and `.CYCLE.h`.
- The primary shared parser module is `src/mermaid_records/operational_raw.py`.
- `src/mermaid_records/cycle_raw.py` remains only as a legacy compatibility shim.
- Code-facing names may still use `cycle` in legacy modules, but the parsed operational record type is `OperationalLogEntry`.
- Code-facing names for upstream decode should make the `BIN` -> `CYCLE` transformation explicit.
- Textual docs/help may still refer to `.CYCLE.h` explicitly.
- Discovery should support upstream/server-style raw inputs separately from processed/reference inputs.
- `LOG` is the native per-dive operational source.
- `CYCLE` and `.CYCLE.h` are derived or stitched operational products and are secondary/reference-oriented relative to raw `LOG`.
- `.CYCLE.h` and `.MER` may still be treated as parallel parser inputs when needed, but processed `.CYCLE.h` is secondary/reference-oriented.
- Their mutual references may be useful later, but parsing must not depend on them matching.
- A single `.MER` may include DET data from the current dive plus REG/REQ data from previous dives. Do not infer dive membership from `MerDataBlock.date` during parsing.
- The Bathymetrix header currently belongs only in `src/mermaid_records/__init__.py` and `src/mermaid_records/cli.py`.

## Current Fixtures

Tracked fixtures currently include:

- `data/fixtures/452.020-P-06/cycle/*.CYCLE`
- `data/fixtures/452.020-P-06/log/*.LOG`
- `data/fixtures/452.020-P-06/mer/*.MER`
- `data/fixtures/452.020-P-06/README.md`
- `data/fixtures/467.174-T-0100/bin/*.BIN`
- `data/fixtures/467.174-T-0100/cycle/*.CYCLE`
- `data/fixtures/467.174-T-0100/log/*.LOG`
- `data/fixtures/467.174-T-0100/mer/*.MER`
- `data/fixtures/467.174-T-0100/s61/*.S61`
- `data/fixtures/467.174-T-0100/README.md`

These fixtures are intentional parser fixtures and should generally remain tracked unless the user decides otherwise.

Current fixture corpus note:

- `data/fixtures/452.020-P-06/` mirrors top-level canonical artifacts for float `452.020-P-06`, grouped by artifact type for easier cross-checking.
- This older-generation float is `LOG`-first, so the fixture family has no `BIN` branch.
- `data/fixtures/467.174-T-0100/` mirrors top-level canonical artifacts for float `467.174-T-0100`, grouped by artifact type for easier cross-checking.
- Raw `BIN` and `MER` files in this fixture family are copied from `~/mermaid/server`.
- Treat this fixture family as the primary local fixture corpus unless the user asks for a different source set.
- `data/fixtures/log_examples_representative_06_0100/` is the current representative LOG subset for JSONL prototype work, and generated JSONL prototypes may live under its `jsonl_prototype/` subdirectory for inspection.
- `data/fixtures/mer_examples_representative_06_0100/` is the current representative MER subset for JSONL prototype work, and generated MER JSONL prototypes may live under its `jsonl_prototype/` subdirectory for inspection.

## Workflow Rules

- If the work shifts toward higher-level API design, abstraction tradeoffs, naming strategy, or broader architecture, say when it may be a good moment to consult ChatGPT and provide a concise handoff summary.
- Do not be territorial about tool choice; suggest ChatGPT when it is likely to help with design-space exploration.

## Module Naming Rule — Action First

All transformation and processing modules should use action-first naming.

Definition:

- start module names with the verb describing what the module does
- follow with the source or target object

Examples:

- `normalize_log.py`
- `normalize_mer.py`
- `parse_log.py`
- `parse_mer.py`

Avoid:

- `log_normalize.py`
- `mer_normalize.py`

Rationale:

- groups modules by behavior rather than data type
- scales cleanly as new pipelines are added
- improves discoverability and consistency across the package

Additional rule:

- keep module names lowercase
- use underscores only when needed for readability
