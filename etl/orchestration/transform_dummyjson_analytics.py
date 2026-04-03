"""Orchestration helpers for customer_analytics and order_analytics generation."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from etl.transformers import AnalyticsArtifact, OrderSource, build_analytics_datasets


def run_dummyjson_analytics_transformation(
    processed_root: Path | None = None,
    orders_root: Path | None = None,
    source: OrderSource = "auto",
    as_of_date: date | str | None = None,
) -> AnalyticsArtifact:
    """Run the analytics transformation stage."""
    kwargs = {
        "source": source,
        "as_of_date": as_of_date,
    }
    if processed_root is not None:
        kwargs["processed_root"] = processed_root
    if orders_root is not None:
        kwargs["orders_root"] = orders_root
    return build_analytics_datasets(**kwargs)
