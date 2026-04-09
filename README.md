# mermaid-timeline

`mermaid-timeline` is a small Python package for conservative MERMAID raw parsing and future timeline interpretation.

The package intentionally separates parsing from interpretation:

- raw operational `LOG` parsing and shared operational-line parsing live in `cycle_raw.py`
- raw `BIN` to emitted `CYCLE` decode remains separate in `bin2cycle.py`
- emitted `CYCLE` and processed `.CYCLE.h` remain supported as operational text inputs
- raw `.MER` parsing lives in `mer_raw.py`
- higher-level interpretation modules stay separate

## Installation

```bash
pip install -e .[dev]
```

## CLI

Inspect a MER file:

```bash
mermaid-timeline inspect-mer /path/to/file.MER
```

Inspect an operational `LOG`, `CYCLE`, or `.CYCLE.h` file:

```bash
mermaid-timeline inspect-cycle /path/to/file.LOG
```

Both commands currently expose conservative parser stubs intended to preserve raw information without overcommitting to a specific decode format.
