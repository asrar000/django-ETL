#!/usr/bin/env python3
"""Generate a synthetic DummyJSON order dataset at a target row count."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.orchestration import run_dummyjson_synthetic_order_generation
from etl.utils import INTERIM_DATA_DIR, LOGS_DIR, RAW_DATA_DIR, configure_logging


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the synthetic generation runner."""
    parser = argparse.ArgumentParser(
        description="Generate a deterministic synthetic DummyJSON order dataset."
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=RAW_DATA_DIR,
        help="Directory containing the extracted raw snapshots.",
    )
    parser.add_argument(
        "--interim-root",
        type=Path,
        default=INTERIM_DATA_DIR,
        help="Directory where the synthetic dataset will be saved.",
    )
    parser.add_argument(
        "--target-rows",
        type=int,
        default=10000,
        help="Target number of synthetic order rows to generate.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the synthetic generation runner.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the synthetic generation job and print a short summary."""
    args = parse_args()
    configure_logging(
        log_level=args.log_level,
        log_file=LOGS_DIR / "generate_dummyjson_synthetic_orders.log",
    )

    artifact = run_dummyjson_synthetic_order_generation(
        raw_root=args.raw_root,
        interim_root=args.interim_root,
        target_rows=args.target_rows,
    )

    print(
        "synthetic_orders: built "
        f"{artifact.record_count} records from {artifact.base_record_count} base orders -> {artifact.output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
