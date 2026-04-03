"""Unit tests for the DummyJSON extraction layer."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from etl.extractors.dummyjson import DummyJSONExtractionError, DummyJSONExtractor


class DummyJSONExtractorTests(unittest.TestCase):
    """Validate pagination, persistence, and record validation."""

    def test_extract_resource_fetches_all_pages_and_writes_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DummyJSONExtractor(raw_root=Path(temp_dir), page_size=1)

            responses = [
                {
                    "carts": [{"id": 1, "userId": 10, "products": [{"id": 100, "quantity": 1}]}],
                    "total": 2,
                },
                {
                    "carts": [{"id": 2, "userId": 11, "products": [{"id": 101, "quantity": 3}]}],
                    "total": 2,
                },
            ]

            with patch.object(DummyJSONExtractor, "_request_json", side_effect=responses):
                artifact = extractor.extract_resource("carts")

            self.assertEqual(artifact.record_count, 2)
            self.assertTrue(artifact.output_path.exists())

            payload = json.loads(artifact.output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["resource"], "carts")
            self.assertEqual(payload["record_count"], 2)
            self.assertEqual(len(payload["records"]), 2)

    def test_extract_resource_raises_for_duplicate_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DummyJSONExtractor(raw_root=Path(temp_dir))

            with patch.object(
                DummyJSONExtractor,
                "_request_json",
                return_value={
                    "products": [
                        {"id": 1, "title": "A", "category": "beauty", "price": 9.99},
                        {"id": 1, "title": "B", "category": "beauty", "price": 19.99},
                    ],
                    "total": 2,
                },
            ):
                with self.assertRaises(DummyJSONExtractionError):
                    extractor.extract_resource("products")


if __name__ == "__main__":
    unittest.main()
