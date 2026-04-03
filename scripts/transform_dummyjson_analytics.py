#!/usr/bin/env python3
"""Build customer_analytics and order_analytics tables from the latest order dataset."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.orchestration import run_dummyjson_analytics_transformation
from etl.utils import INTERIM_DATA_DIR, LOGS_DIR, PROCESSED_DATA_DIR, configure_logging


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the analytics transformation runner."""
    parser = argparse.ArgumentParser(
        description="Build customer_analytics and order_analytics from the latest order dataset."
    )
    parser.add_argument(
        "--orders-root",
        type=Path,
        default=INTERIM_DATA_DIR / "orders",
        help="Directory containing the interim order datasets.",
    )
    parser.add_argument(
        "--processed-root",
        type=Path,
        default=PROCESSED_DATA_DIR,
        help="Directory where analytics datasets will be saved.",
    )
    parser.add_argument(
        "--source",
        choices=("auto", "synthetic", "enriched"),
        default="auto",
        help="Which interim order dataset type to transform.",
    )
    parser.add_argument(
        "--as-of-date",
        help="As-of date for customer_tenure_days in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the analytics runner.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the analytics transformation job and print a short summary."""
    args = parse_args()
    configure_logging(
        log_level=args.log_level,
        log_file=LOGS_DIR / "transform_dummyjson_analytics.log",
    )

    artifact = run_dummyjson_analytics_transformation(
        processed_root=args.processed_root,
        orders_root=args.orders_root,
        source=args.source,
        as_of_date=args.as_of_date,
    )

    print(
        "customer_analytics: built "
        f"{artifact.customer_record_count} records -> {artifact.customer_output_path}"
    )
    print(
        "order_analytics: built "
        f"{artifact.order_record_count} records -> {artifact.order_output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
