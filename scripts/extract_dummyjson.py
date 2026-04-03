#!/usr/bin/env python3
"""Run the DummyJSON raw extraction stage."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.orchestration import run_dummyjson_extraction
from etl.utils import LOGS_DIR, RAW_DATA_DIR, configure_logging


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the extraction runner."""
    parser = argparse.ArgumentParser(description="Extract raw carts, users, and products from DummyJSON.")
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=RAW_DATA_DIR,
        help="Directory where raw snapshots will be saved.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of rows requested per API page.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout in seconds for each API request.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="Maximum number of attempts for retryable API failures.",
    )
    parser.add_argument(
        "--backoff-factor",
        type=float,
        default=1.0,
        help="Base exponential backoff delay in seconds.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the extraction runner.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the extraction job and print a short summary."""
    args = parse_args()
    configure_logging(
        log_level=args.log_level,
        log_file=LOGS_DIR / "extract_dummyjson.log",
    )

    artifacts = run_dummyjson_extraction(
        raw_root=args.raw_root,
        page_size=args.page_size,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        backoff_factor=args.backoff_factor,
    )

    for artifact in artifacts:
        print(
            f"{artifact.resource_name}: extracted {artifact.record_count} records -> {artifact.output_path}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
