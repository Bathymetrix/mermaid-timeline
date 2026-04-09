# SPDX-License-Identifier: MIT

"""Generate prototype JSONL streams from the representative LOG subset."""

from __future__ import annotations

import argparse
from pathlib import Path

from mermaid_timeline.operational_jsonl import write_log_jsonl_prototypes


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate prototype LOG-derived JSONL record families.",
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("data/fixtures/log_examples_representative_06_0100"),
        help="Root directory containing representative LOG fixtures.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/fixtures/log_examples_representative_06_0100/jsonl_prototype"),
        help="Directory where prototype JSONL files will be written.",
    )
    args = parser.parse_args()

    log_paths = sorted(args.input_root.rglob("*.LOG"))
    summary = write_log_jsonl_prototypes(log_paths, args.output_dir)

    print(f"Processed {summary.total_records} parsed LOG records.")
    print(f"  operational_records.jsonl: {summary.operational_records}")
    print(f"  acquisition_records.jsonl: {summary.acquisition_records}")
    print(f"  transmission_records.jsonl: {summary.transmission_records}")
    print(f"  measurement_records.jsonl: {summary.measurement_records}")
    print(
        "  unclassified_operational_records.jsonl: "
        f"{summary.unclassified_records}"
    )

    print("Acquisition counts by state:")
    for key, value in sorted(summary.acquisition_state_counts.items()):
        print(f"  {key}: {value}")

    print("Acquisition counts by evidence kind:")
    for key, value in sorted(summary.acquisition_evidence_kind_counts.items()):
        print(f"  {key}: {value}")

    _print_acquisition_examples(summary.acquisition_examples)
    _print_examples("Transmission examples", summary.transmission_examples)
    _print_examples("Measurement examples", summary.measurement_examples)
    _print_examples("Unclassified examples", summary.unclassified_examples)

    print("Most common unclassified patterns:")
    for pattern in summary.common_unclassified_patterns[:10]:
        print(
            "  "
            f"{pattern['count']}x "
            f"[{pattern['subsystem']},{pattern['code']}] "
            f"{pattern['message']}"
        )

    return 0


def _print_examples(title: str, records: list[dict[str, object]]) -> None:
    print(f"{title}:")
    if not records:
        print("  (none)")
        return
    for record in records:
        print(f"  - {record['time']} {record['message']}")


def _print_acquisition_examples(
    examples: dict[str, dict[str, object]],
) -> None:
    print("Acquisition examples:")
    ordered_keys = [
        "started:transition",
        "stopped:transition",
        "started:assertion",
        "stopped:assertion",
    ]
    for key in ordered_keys:
        record = examples.get(key)
        if record is None:
            print(f"  - {key}: (none)")
            continue
        print(f"  - {key}: {record['time']} {record['message']}")


if __name__ == "__main__":
    raise SystemExit(main())
