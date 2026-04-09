# mermaid-timeline

`mermaid-timeline` is a small Python package for conservative parsing, decode adapters, discovery, audit, and normalization of MERMAID artifacts.

The package intentionally separates decode, parsing, normalization-oriented extraction, and later interpretation:

- canonical upstream sources currently include `BIN`, `LOG`, and `MER`
- raw `BIN` to emitted `CYCLE` decode remains separate in `bin2cycle.py`
- shared operational text parsing for `LOG`, `CYCLE`, and `.CYCLE.h` lives in `cycle_raw.py`
- raw `.MER` parsing lives in `mer_raw.py`
- audit and discovery layers stay separate from parsing
- higher-level interpretation modules remain intentionally minimal for now

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
