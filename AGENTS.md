# AGENTS

## Persistent Rules

- Never mix parsing and interpretation.
- Keep record interfaces stable.
- Always preserve unknown MER record types.
- Prefer generators for file parsing where practical.
- Add or update tests with any real logic change.
- Prefer small, coherent commits and always tell the user when the current state is a good time to commit.
- Treat this file as the persistent handoff document for future Codex sessions and update it whenever assumptions, boundaries, fixtures, or workflow rules change.
- Add the exact Bathymetrix header only to `src/mermaid_timeline/__init__.py` and `src/mermaid_timeline/cli.py`.
- Do not add the Bathymetrix header to `README.md`, tests, or internal implementation modules unless the user explicitly asks.
- When referring to an output cycle file, call it `CYCLE` (parallel to `BIN`, `LOG`, and `MER`).
- Never delete source raw files from their original path. This includes `.BIN`, `.MER`, and future raw formats such as `.vit`. If an external decoder is destructive, it must run only on copied files in a temporary workspace.
- In the corpus, raw input data live in `server`, and manufacturer-produced output used for audit/reference comparison lives in `processed`.
- Before writing a new audit workflow, first scan existing audit code for reusable logic and easy refactors instead of starting from a fresh script by default.
- When the user's intent is clear, rewrite their rules into cleaner internal guidance here rather than copying rough wording verbatim. If the intent is unclear, ask before recording the rule.
- Package license defaults to MIT. A root `LICENSE` file must exist.
- Every Python source file must include `SPDX-License-Identifier: MIT`, but must not include the full license text.
- The exact Bathymetrix header belongs only in `src/mermaid_timeline/__init__.py` and `src/mermaid_timeline/cli.py`, unless the user explicitly expands that set.
- Use `Bathymetrix™` only in approved file headers and rare top-level visible branding. Do not use the trademark marker in identifiers, module names, imports, docstrings, or inline comments.
- Package authorship/licensing metadata must live in exactly one location: `src/mermaid_timeline/__init__.py` via `__author__`, `__license__`, and `__copyright__`.

## Project Purpose

`mermaid-timeline` is a Python package for conservative parsing of MERMAID timeline-related inputs.

Supported inputs currently include:

- `.BIN` files
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

## Current Parsing Scope

Canonical long-term source model:

- upstream raw `.BIN`
- upstream raw `.MER`

Processed `.CYCLE.h` remains supported as a compatibility, reference, and comparison path rather than the canonical long-term source.

Decode/parsing boundary:

- decode: raw `BIN` -> emitted raw `CYCLE` text
- parsing: consume emitted raw `CYCLE` text or processed `.CYCLE.h`
- interpretation/timeline logic remains separate and should not be mixed into either layer

### `.CYCLE.h`

Parse `.CYCLE.h` files into `CycleLogEntry` records with:

- `time`
- `subsystem`
- `code`
- `message`
- `raw_line`
- `source_file`

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

- Code-facing names should use `cycle` rather than `log` for `.CYCLE.h` parsing.
- Code-facing names for upstream decode should make the `BIN` -> `CYCLE` transformation explicit.
- Textual docs/help may still refer to `.CYCLE.h` explicitly.
- Discovery should support upstream/server-style raw inputs separately from processed/reference inputs.
- `.CYCLE.h` and `.MER` may still be treated as parallel parser inputs when needed, but processed `.CYCLE.h` is secondary/reference-oriented.
- Their mutual references may be useful later, but parsing must not depend on them matching.
- The Bathymetrix header currently belongs only in `src/mermaid_timeline/__init__.py` and `src/mermaid_timeline/cli.py`.

## Current Fixtures

Tracked fixtures currently include:

- `data/fixtures/0075_6858665E.CYCLE.h`
- `data/fixtures/0100_685864F3.MER`
- `data/fixtures/0100_6872AF2F.BIN`
- `data/fixtures/0100_687FDFA4.BIN`
- `data/fixtures/0100_688CFEFC.BIN`
- `data/fixtures/0100_689A203B.BIN`
- `data/fixtures/0100_68A75083.BIN`

These fixtures are intentional parser fixtures and should generally remain tracked unless the user decides otherwise.

## Workflow Rules

- If the work shifts toward higher-level API design, abstraction tradeoffs, naming strategy, or broader architecture, say when it may be a good moment to consult ChatGPT and provide a concise handoff summary.
- Do not be territorial about tool choice; suggest ChatGPT when it is likely to help with design-space exploration.
