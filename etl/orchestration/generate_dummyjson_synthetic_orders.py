"""Orchestration helpers for the DummyJSON synthetic generation stage."""

from __future__ import annotations

from pathlib import Path

from etl.transformers import SyntheticOrdersArtifact, build_synthetic_orders_dataset


def run_dummyjson_synthetic_order_generation(
    raw_root: Path | None = None,
    interim_root: Path | None = None,
    target_rows: int = 10000,
) -> SyntheticOrdersArtifact:
    """Run the transform step that generates synthetic orders from DummyJSON data."""
    kwargs = {"target_rows": target_rows}
    if raw_root is not None:
        kwargs["raw_root"] = raw_root
    if interim_root is not None:
        kwargs["interim_root"] = interim_root
    return build_synthetic_orders_dataset(**kwargs)
