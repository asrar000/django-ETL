#!/usr/bin/env python3
"""Build an enriched DummyJSON order dataset from the latest raw snapshots."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.orchestration import run_dummyjson_order_enrichment
from etl.utils import INTERIM_DATA_DIR, LOGS_DIR, RAW_DATA_DIR, configure_logging


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the enrichment runner."""
    parser = argparse.ArgumentParser(
        description="Build an enriched DummyJSON order dataset from the latest raw snapshots."
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
        help="Directory where the enriched dataset will be saved.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the enrichment runner.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the enrichment job and print a short summary."""
    args = parse_args()
    configure_logging(
        log_level=args.log_level,
        log_file=LOGS_DIR / "enrich_dummyjson_orders.log",
    )

    artifact = run_dummyjson_order_enrichment(
        raw_root=args.raw_root,
        interim_root=args.interim_root,
    )

    print(f"enriched_orders: built {artifact.record_count} records -> {artifact.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
