"""Orchestration helpers for the DummyJSON enrichment stage."""

from __future__ import annotations

from pathlib import Path

from etl.transformers import EnrichmentArtifact, build_enriched_orders_dataset


def run_dummyjson_order_enrichment(
    raw_root: Path | None = None,
    interim_root: Path | None = None,
) -> EnrichmentArtifact:
    """Run the transform step that enriches raw DummyJSON orders."""
    kwargs = {}
    if raw_root is not None:
        kwargs["raw_root"] = raw_root
    if interim_root is not None:
        kwargs["interim_root"] = interim_root
    return build_enriched_orders_dataset(**kwargs)
