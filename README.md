# mermaid-timeline

`mermaid-timeline` is a small Python package scaffold for low-level MER data ingestion and future timeline interpretation.

The package intentionally separates parsing from interpretation:

- raw `.MER` parsing lives in `mer_raw.py`
- `.CYCLE.h` text parsing lives in `cycle_raw.py`
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

Inspect a `.CYCLE.h` file:

```bash
mermaid-timeline inspect-cycle /path/to/file.CYCLE.h
```

Both commands currently expose conservative parser stubs intended to preserve raw information without overcommitting to a specific decode format.
