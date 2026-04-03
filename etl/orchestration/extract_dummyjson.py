"""Orchestration helpers for the DummyJSON extraction stage."""

from __future__ import annotations

from pathlib import Path

from etl.extractors import DummyJSONExtractor, ExtractionArtifact
from etl.utils.paths import RAW_DATA_DIR


def run_dummyjson_extraction(
    raw_root: Path | None = None,
    page_size: int = 100,
    timeout_seconds: int = 30,
    max_retries: int = 4,
    backoff_factor: float = 1.0,
) -> list[ExtractionArtifact]:
    """Run the raw extraction stage for carts, users, and products."""
    extractor = DummyJSONExtractor(
        raw_root=raw_root if raw_root is not None else RAW_DATA_DIR,
        page_size=page_size,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
    )
    return extractor.extract_all()
