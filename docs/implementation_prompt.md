# Follow-Up Implementation Prompt

Use this prompt when extending `mermaid-timeline` beyond the initial interval
synthesis package:

```text
You are working in the mermaid-timeline package. Preserve the current JSONL
interval philosophy: one flat interval object per line, grouped by interval
record shape rather than by nested documents.

Before changing behavior, read docs/schema.md and the tests under tests/.
Keep buf intervals sourced only from log_acquisition_records.jsonl, and keep
det/req intervals sourced only from mer_event_records.jsonl.

Next task:
- add the requested feature while preserving the current schema_version unless
  the output contract changes;
- add focused tests for any state-machine, validation, or timing change;
- keep waveform analysis out of this package;
- leave GCMT/catalog/travel-time joins for mermaid-gcmt unless the task is
  only preparing an export consumed by that package.
```
