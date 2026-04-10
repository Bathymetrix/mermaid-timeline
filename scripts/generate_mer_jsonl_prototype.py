# SPDX-License-Identifier: MIT

"""Generate prototype JSONL streams from the representative MER subset."""

from __future__ import annotations

import argparse
from pathlib import Path

from mermaid_records.normalize_mer import write_mer_jsonl_prototypes


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate prototype MER-derived JSONL record families.",
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("data/fixtures/mer_examples_representative_06_0100"),
        help="Root directory containing representative MER fixtures.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/fixtures/mer_examples_representative_06_0100/jsonl_prototype"),
        help="Directory where prototype JSONL files will be written.",
    )
    args = parser.parse_args()

    mer_paths = sorted(args.input_root.rglob("*.MER"))
    summary = write_mer_jsonl_prototypes(mer_paths, args.output_dir)

    print(f"Processed {summary.total_mer_files} MER files.")
    print(f"  mer_environment_records.jsonl: {summary.environment_records}")
    print(f"  mer_parameter_records.jsonl: {summary.parameter_records}")
    print(f"  mer_data_records.jsonl: {summary.data_records}")
    print("Environment counts by kind:")
    for key, value in sorted(summary.environment_kind_counts.items()):
        print(f"  {key}: {value}")
    print("Parameter counts by kind:")
    for key, value in sorted(summary.parameter_kind_counts.items()):
        print(f"  {key}: {value}")
    print(f"Total event blocks: {summary.total_event_blocks}")
    print(f"Files with zero event blocks: {summary.zero_event_files}")
    print(f"Unknown environment tags: {summary.unknown_environment_tags or 'none'}")
    print(f"Unknown parameter tags: {summary.unknown_parameter_tags or 'none'}")
    print(f"Unknown INFO keys: {summary.unknown_info_keys or 'none'}")
    print(f"Unknown FORMAT keys: {summary.unknown_format_keys or 'none'}")
    _print_example("GPSINFO environment example", summary.example_gpsinfo_environment)
    _print_example("DRIFT environment example", summary.example_drift_environment)
    _print_example("ADC parameter example", summary.example_adc_parameter)
    _print_example("MODEL parameter example", summary.example_model_parameter)
    _print_example("Data block with FNAME/SMP_OFFSET/TRUE_FS", summary.example_data_with_fname)
    _print_example(
        "Data block with PRESSURE/TEMPERATURE/CRITERION/SNR/TRIG/DETRIG",
        summary.example_data_with_trigger_fields,
    )

    return 0


def _print_example(title: str, record: dict[str, object] | None) -> None:
    print(f"{title}:")
    if record is None:
        print("  (none)")
        return
    for key in sorted(record):
        print(f"  {key}: {record[key]}")


if __name__ == "__main__":
    raise SystemExit(main())
