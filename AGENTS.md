# AGENTS

- Never mix parsing and interpretation.
- Always preserve unknown MER record types.
- Keep record interfaces stable.
- Add or update tests with any real logic change.
- Prefer generators for file parsing.
- Always tell the user when the current state is a good time to commit.
- Treat this file as the persistent handoff document for future Codex sessions.
- Update or rewrite this file whenever project assumptions, parsing boundaries, fixtures, or workflow rules change.

## Project Purpose

`mermaid-timeline` is a Python package for conservative parsing of MERMAID timeline-related inputs.

The current raw inputs are:

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
- Textual docs/help may still refer to `.CYCLE.h` explicitly.
- `.CYCLE.h` and `.MER` should be treated as parallel raw inputs.
- Their mutual references may be useful later, but parsing must not depend on them matching.

## Current Fixtures

Tracked fixtures currently include:

- `data/fixtures/0075_6858665E.CYCLE.h`
- `data/fixtures/0100_685864F3.MER`

These fixtures are intentional parser fixtures and should generally remain tracked unless the user decides otherwise.

## Workflow Rules

- Prefer small, coherent commits.
- When a rename/refactor/test-backed parsing slice is complete, explicitly tell the user it is a good time to commit.
- If the work shifts toward higher-level API design, abstraction tradeoffs, naming strategy, or broader architecture, say when it may be a good moment to consult ChatGPT and provide a concise handoff summary.
- Do not be territorial about tool choice; suggest ChatGPT when it is likely to help with design-space exploration.
