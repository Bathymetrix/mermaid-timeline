# Limitations

## Manifest and state artifacts depend on mode

`Stateful` and `stateless` runs do not persist the same side artifacts.

Stateful mode:

- writes `manifests/latest.json`
- writes one unique `manifests/runs/<run_id>/...` directory per executed run
- writes `state/pruned_records.jsonl`
- persists malformed/skipped-source recovery artifacts in the per-run manifest directory

Stateless mode:

- writes no `manifests/`
- writes no `state/`
- does not persist malformed/skipped-source recovery artifacts separately from the normalized JSONL outputs
- cannot target an output tree that already contains `manifests/`

`preflight_status.json` is different from manifests: it is written only when BIN decode preflight runs with a durable instrument output directory, regardless of whether the run is stateful or stateless.

## MER event preservation is structured, not verbatim

MER event normalization does not preserve the full original `<EVENT>...</EVENT>` block as one byte-for-byte field.

Successful normalized event rows preserve structured components instead:

- `raw_info_line`
- `raw_format_line` when a `<FORMAT>` line exists
- `encoded_payload`
- payload accounting fields such as `encoded_payload_byte_count`, `data_payload_nbytes`, and `payload_length_matches_expected`

Important consequences:

- downstream consumers should not expect exact reconstruction of the original event block from one stored verbatim field
- Stanford PSD event blocks that omit `<FORMAT>` are still valid and normalize with `raw_format_line = null`
- payload byte counts measure only the bytes inside `<DATA>...</DATA>` and exclude surrounding framing whitespace

## Allowed transformations in v1

Normalization is conservative, but it is not a raw byte dump. The following transformations are intentionally allowed:

- line-read newline normalization such as stripping trailing `\r\n`
- canonicalizing parsed LOG/MER filename references by replacing `/` with `_`
- canonicalizing parsed LOG rollover targets to normalized `.LOG` filenames
- parsing source text into explicit structured fields without adding inferred interpretation
- resolving canonical `instrument_id` values from recognized serial naming rules when available
- materializing the canonical top-level JSONL file set as empty files when a family has no rows

No additional interpretation-oriented transformations are part of the v1 contract. In particular, the normalization layer does not do coordinate conversion, derived intervals, mission inference, or waveform interpretation.

## Mode-specific rerun limits

`Stateful` mode can append, rewrite, noop, and prune because it has persisted source state.

`Stateless` mode cannot do that safely because it has no manifests. Its rerun contract is therefore intentionally narrower:

- reruns rewrite the targeted package-owned family outputs
- reruns do not append to prior stateless JSONL files
- reruns do not silently duplicate rows

## Fixture-backed coverage is partial

The tracked release-facing fixtures exercise important current cases, including:

- older-generation direct `LOG` + `MER` data
- BIN-backed families with decoded `LOG` fixtures
- compact real PSD / Stanford-style raw `MER` examples, including a metadata-only file and event blocks without `<FORMAT>`

They do not prove coverage for every float generation, every external decoder behavior, or every malformed raw artifact pattern seen in the field.

## External decoder boundary

`BIN` handling still depends on an external preprocess/decode workflow. `mermaid-records` does not replace that decoder, and any decoder-environment failures, database update issues, or upstream decode differences remain outside the normalization layer itself.
