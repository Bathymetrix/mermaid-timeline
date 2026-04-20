# Ethos

## Core principle

`mermaid-records` is a normalization layer, not an interpretation or analysis layer.

Its role is to transform raw MERMAID artifacts into stable, machine-readable records without introducing higher-level meaning. It should expose structure, not infer meaning.

## Design goals

The v1 design goals are:

- preserve source information conservatively
- avoid silent data loss
- maintain predictable, stable output structure
- provide a durable baseline for downstream tooling
- keep normalization behavior explicit and auditable

## Scope discipline

The package must resist expansion into:

- scientific interpretation
- derived metrics or intervals
- coordinate conversion
- waveform analysis
- mission-level synthesis
- workflow orchestration beyond normalization

These are not deferred features of the same layer. They belong in higher-level tooling.

## Stability

The intended long-term posture is that this layer becomes relatively static.

Changes should mainly be driven by:

- newly encountered float generations or raw formats
- correctness fixes
- clearly exposed shortcomings in baseline normalization

Changes should **not** be driven by pressure to add interpretation or convenience derivations to this layer.

## Philosophy

The package follows a conservative style:

- parse, do not interpret
- expose, do not summarize
- preserve, do not transform unless structure requires it
- make edge cases visible rather than smoothing them away

## Operational implication

This boundary should be treated as a design constraint, not a suggestion. If a proposed feature introduces interpretation, derived semantics, or analysis-oriented convenience, it likely belongs outside `mermaid-records`.
