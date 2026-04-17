# AGENTS

## Persistent Rules

- Never mix parsing and interpretation.
- Keep record interfaces stable.
- Always preserve unknown MER record types.
- Prefer generators for file parsing where practical.
- Add or update tests with any real logic change.
- Prefer small, coherent commits and always tell the user when the current state is a good time to commit.
- Any time suggesting a commit, also suggest a matching commit message.
- Treat this file as the persistent handoff document for future Codex sessions and update it when assumptions, boundaries, fixtures, or workflow rules change.
- Add the exact Bathymetrix header only to `src/mermaid_records/__init__.py` and `src/mermaid_records/cli.py`.
- Do not add the Bathymetrix header to `README.md`, tests, or internal implementation modules unless the user explicitly asks.
- Never delete source raw files from their original path. This includes `.BIN`, `.LOG`, `.MER`, and future raw formats such as `.vit`. If an external decoder is destructive, it must run only on copied files in a temporary workspace.
- Before writing a new audit workflow, first scan existing audit code for reusable logic and easy refactors instead of starting from a fresh script by default.
- Keep code DRY, but verify behavior before and after any DRY-driven refactor instead of assuming equivalence.
- Always push back, disagree, or suggest a better alternative when warranted.
- Package license defaults to MIT. A root `LICENSE` file must exist.
- Every Python source file must include `SPDX-License-Identifier: MIT`, but must not include the full license text.
- Use `Bathymetrix™` only in approved file headers and rare top-level visible branding. Do not use the trademark marker in identifiers, module names, imports, docstrings, or inline comments.
- Package authorship/licensing metadata must live in exactly one location: `src/mermaid_records/__init__.py` via `__author__`, `__license__`, and `__copyright__`.
- `CYCLE` / `CYCLE.h` are out of scope for `mermaid-records` and must not be reintroduced unless the user explicitly requests it.

## Versioning Rule

- The canonical version must live in `src/mermaid_records/__init__.py` as `__version__`.
- `pyproject.toml` must use dynamic versioning via `[tool.setuptools.dynamic]` with `version = {attr = "mermaid_records.__version__"}`.
- No other hardcoded package version strings are allowed in the repo.

## Project Purpose

`mermaid-records` is a Python package to normalize `BIN`, `LOG`, and `MER` into parseable record families.

Supported inputs currently include:

- `.BIN` files
- `.LOG` files
- `.MER` files

This layer is strictly for parsing and structured extraction.

Do not add:

- unit conversions
- coordinate conversions
- inferred durations from sample counts and rates
- acquisition inference beyond explicit `acq started` / `acq stopped`
- DET / REQ logic
- higher-level timeline interpretation
- workflow-engine behavior beyond the normalization pipeline

## Current Parsing Scope

Canonical source model:

- upstream raw `.BIN`
- upstream raw `.LOG`
- upstream raw `.MER`

Decode/parsing boundary:

- decode: raw `BIN` -> decoded `LOG`
- parsing: consume raw `LOG` and raw `MER`
- interpretation/timeline logic remains separate and should not be mixed into either layer

For v1 normalization work, the canonical decode seam is `BIN` -> `LOG`.

Mirror the upstream preprocess call order responsibly:

- `database_update(...)` is a batch preflight step
- `concatenate_files(...)` and `decrypt_all(...)` are part of the per-workspace `BIN` -> `LOG` decode path
- `concatenate_rbr_files(...)` may also be part of preprocessing, but should not force interpretation into the decode layer

Do not call `database_update(...)` once per `BIN`; prefer a single explicit refresh before a batch decode workflow.
Preflight policy is mode-dependent:

- `strict`: fail closed
- `cached`: allow explicit degraded continuation when live refresh fails, and record/report that degraded preflight state clearly

### Operational Text Sources

Use one common `OperationalLogEntry` model for `LOG` with:

- `time`
- `subsystem`
- `code`
- `message`
- `source_kind`
- `raw_line`
- `source_file`

Preserve source identity via `source_kind = "log"`.

Normalized record-family direction to keep in mind during cleanup and naming:

- `operational_records`
- `location_records`
- `transmission_records`
- `acquisition_records`
- `ascent_request_records`
- `parameter_records`
- `testmode_records`
- `sbe_records`
- `mer_event_blocks`
- `acquisition_intervals`
- `pressure_temperature_records`
- `battery_records`
- `gps_records`
- `unclassified_operational_records`

For LOG-derived measurement-adjacent lines:

- dedicated `log_pressure_temperature_records.jsonl` is only for literal `P...mbar,T...mdegC` observations with parsed `pressure_mbar` and `temperature_mdegc`
- dedicated `log_battery_records.jsonl` is only for literal `battery ...mV, ...uA` telemetry with parsed `voltage_mv` and `current_ua`
- other lines that were formerly routed into `log_measurement_records.jsonl` now remain only in `log_operational_records.jsonl`; do not expand their parsing without an explicit request

For normalized LOG/MER family boundaries:

- split files by coherent subsystem or workflow, not by the presence of a primary scalar field
- keep structurally different line kinds together when they describe one process or state machine, using internal `*_kind` fields instead of fragmenting them into one-file-per-scalar outputs
- avoid reviving vague mixed-domain buckets like the former measurement family

For derived operational-family prototypes, no parsed `OperationalLogEntry` should disappear silently. Each parsed operational line must end up either in exactly one derived family stream or in `unclassified_operational_records`.

Operational-family routing contract:

- grouped structural LOG routes such as `parameter`, `testmode`, and `sbe` are resolved before ordinary `OperationalLogEntry` family classification
- every ordinary `OperationalLogEntry` always emits one record to `log_operational_records.jsonl`
- after that, an ordinary operational line may match zero or one derived family
- zero derived matches route to `log_unclassified_records.jsonl`
- two or more derived matches are a normalization bug and must fail loudly; do not hide them with precedence or multi-family emission

For acquisition evidence prototypes, preserve the distinction between exact transitions and state assertions:

- `acq started` / `acq stopped` are transition evidence
- `acq already started` / `acq already stopped` are assertion evidence

Do not infer intervals from assertion lines in the normalization layer.

For ascent-request prototypes, classify only explicit request outcomes such as `ascent request accepted` and `ascent request rejected`. Do not infer ascent-request state from other ascent-related lines.

For GPS prototypes, emit one record per clearly GPS-related LOG line such as `GPS fix...`, raw latitude/longitude lines, `hdop`/`vdop`, `GPSACK`, and `GPSOFF`. Do not group lines into fixes or compute derived position/timing values in the normalization layer.

For rollover banner lines like `timestamp:*** switching to 0026/5D4A3E75 ***`, preserve them as operational LOG records instead of malformed lines and emit `switched_to_log_file` with canonical filename normalization.

Whenever LOG content is parsed as a LOG or MER filename reference, canonicalize only that parsed filename by replacing `/` with `_`. If the context implies a LOG rollover target, emit the canonical `.LOG` filename form.

For generated JSONL filenames, prefix LOG-derived outputs with `log_` and reserve analogous `mer_` prefixes for MER-derived outputs.

Acquisition windows may be extracted only from explicit:

- `acq started`
- `acq stopped`

Ignore lines like:

- `acq already started`
- `acq already stopped`

### `.MER`

Parse `.MER` files into:

- one `MerFileMetadata`
- zero or more `MerEventBlock` values

For metadata:

- preserve raw `ENVIRONMENT` lines
- preserve raw `PARAMETERS` lines
- extract repeated `GPSINFO`, `DRIFT`, and `CLOCK` structures conservatively
- keep GPS coordinates in raw string form
- valid Stanford PSD `.MER` files may contain `<ENVIRONMENT>` + `<PARAMETERS>` and zero `<EVENT>` blocks; normalize these cleanly without treating them as malformed

For event blocks:

- parse `INFO`
- parse `FORMAT` when present, but allow valid Stanford PSD event blocks that contain `INFO` + `DATA` without `FORMAT`
- preserve `DATA` payload bytes without waveform interpretation
- for payload byte counts, measure only the bytes strictly inside `<DATA> ... </DATA>` and exclude surrounding framing bytes such as `\n`, `\r`, and `\t`

## Current File/Layout Assumptions

- The primary shared LOG parser module is `src/mermaid_records/parse_log.py`.
- Discovery should cover only raw `BIN`, `LOG`, and `MER` inputs relevant to this package.
- `LOG` is the native per-dive operational source.
- A single `.MER` may include DET data from the current dive plus REG/REQ data from previous dives. Do not infer dive membership from `MerEventBlock.date` during parsing.
- The normalize CLI matrix audit helper lives in `src/mermaid_records/audit_normalize_cli.py` with the repo script wrapper at `scripts/audit_normalize_cli_matrix.py`.
- The normalize CLI matrix audit defaults to a semantic flag matrix; exhaustive boolean expansion is opt-in.

## Current Fixtures

Tracked fixtures currently include:

- `data/fixtures/452.020-P-06/log/*.LOG`
- `data/fixtures/452.020-P-06/mer/*.MER`
- `data/fixtures/452.020-P-06/README.md`
- `data/fixtures/467.174-T-0100/bin/*.BIN`
- `data/fixtures/467.174-T-0100/log/*.LOG`
- `data/fixtures/467.174-T-0100/mer/*.MER`
- `data/fixtures/467.174-T-0100/s61/*.S61`
- `data/fixtures/467.174-T-0100/README.md`

The representative JSONL prototype fixture sets under `data/fixtures/log_examples_representative_06_0100/` and `data/fixtures/mer_examples_representative_06_0100/` remain in scope for inspection.

## Workflow Rules

- If work shifts toward higher-level API design, abstraction tradeoffs, naming strategy, or broader architecture, say when it may be a good moment to consult ChatGPT and provide a concise handoff summary.
- Do not be territorial about tool choice; suggest ChatGPT when it is likely to help with design-space exploration.
- The normalize CLI matrix audit workflow should isolate each run in its own output sandbox, capture `stdout`/`stderr` per run, append one JSONL result row per attempt, and keep going after failures.
- For BIN-backed audit runs, preserve a real decoder `MERMAID` root for automaid while keeping output-resolution tests sandboxed by seeding only the isolated audit root `database/` link when needed.

## Module Naming Rule

All transformation and processing modules should use action-first naming.

Examples:

- `normalize_log.py`
- `normalize_mer.py`
- `parse_log.py`
- `parse_mer.py`

Avoid:

- `log_normalize.py`
- `mer_normalize.py`

## Normalization Guardrails

- Always preserve the original source representation.
- Always include the raw source line or block where applicable.
- Do not drop, merge, or reinterpret source records silently.
- Prefer source-literal field names when they exist.
- One JSONL record should correspond to one real source unit.
- Normalize structure, not meaning.

## Current Pipeline Rules

- The normalization pipeline has two execution modes:
  - `stateful`: directory input, manifests enabled, incremental rerun logic enabled
  - `stateless`: explicit file-list input, no manifests, no incremental logic, no pruning
- Stateless mode must error if the target output directory already contains manifests.
- Stateful incremental behavior is binary and conservative:
  - append only when the only change is newly added raw source files
  - rewrite when any previously seen raw source changes or is removed
  - decoder-state changes invalidate only BIN-derived outputs for BIN-dependent floats
  - explicit force-rewrite mode may override incremental append/noop decisions and force targeted family rewrites
- JSONL outputs use deterministic processing order, not time-order.
- Normalized JSONL outputs should use basename-only `source_file`; richer full-path provenance belongs in manifests and other run-side artifacts.
- Do not mutate existing JSONL outputs in place; append and full rewrite are the only safe modification paths.
- `--force-rewrite` must remove package-owned generated artifacts for each targeted instrument before regeneration: all top-level `log_*.jsonl`, all top-level `mer_*.jsonl`, and package-owned bookkeeping under `manifests/` and `state/`. Do not delete unknown files or the whole instrument directory.
- Every per-instrument output directory must materialize the canonical output file set even when some families are empty. At minimum this means all top-level LOG and MER JSONL family files must exist as empty files when they have no records; in `stateful` mode also keep the state/manifest scaffold present for that instrument, while `stateless` mode still must not create manifests.
- Future dry-run/report behavior must be completely side-effect free, including no file writes of any kind.
- Persisted `manifests/runs/<run_id>/input_file_diffs.jsonl` is a strict raw input diff log: file-level fields only, no append/rewrite/noop semantics, and no standalone non-file invalidation records.
- Canonical `instrument_id` should be parsed from the Osean serial naming rules when a full serial is available, for example `452.020-P-08 -> P0008` and `467.174-T-0100 -> T0100`. Do not derive canonical `instrument_id` independently in multiple modules.
