"""Unit tests for the DummyJSON synthetic order generator."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

from etl.transformers.dummyjson_synthetic_orders import (
    build_synthetic_orders_dataset,
    generate_synthetic_orders,
    generate_synthetic_orders_frame,
)


class DummyJSONSyntheticOrdersTests(unittest.TestCase):
    """Validate deterministic 10000-row synthetic dataset generation behavior."""

    def test_generate_synthetic_orders_frame_scales_to_target_rows(self) -> None:
        carts, users, products = self._sample_payloads()

        frame = generate_synthetic_orders_frame(
            carts=carts,
            users=users,
            products=products,
            target_rows=5,
        )

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertEqual(len(frame), 5)
        self.assertEqual(frame["order_id"].nunique(), 5)
        self.assertTrue((frame["total_items"] > 0).all())

    def test_generate_synthetic_orders_is_deterministic(self) -> None:
        carts, users, products = self._sample_payloads()

        first = generate_synthetic_orders(
            carts=carts,
            users=users,
            products=products,
            target_rows=5,
        )
        second = generate_synthetic_orders(
            carts=carts,
            users=users,
            products=products,
            target_rows=5,
        )

        self.assertEqual(first, second)
        self.assertEqual(len(first), 5)

        signup_date = datetime.fromisoformat(first[0]["customer"]["signup_date"])
        first_order_timestamp = min(
            datetime.fromisoformat(order["order_timestamp"].replace("Z", "+00:00"))
            for order in first
            if order["customer"]["id"] == first[0]["customer"]["id"]
        )
        self.assertLess(signup_date.date().isoformat(), first_order_timestamp.date().isoformat())

    def test_build_synthetic_orders_dataset_writes_target_row_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_root = root / "raw"
            interim_root = root / "interim"
            carts, users, products = self._sample_payloads()

            self._write_raw_snapshot(
                raw_root / "carts" / "dummyjson_carts_20260403T000000Z.json",
                "carts",
                carts,
            )
            self._write_raw_snapshot(
                raw_root / "users" / "dummyjson_users_20260403T000000Z.json",
                "users",
                users,
            )
            self._write_raw_snapshot(
                raw_root / "products" / "dummyjson_products_20260403T000000Z.json",
                "products",
                products,
            )

            artifact = build_synthetic_orders_dataset(
                raw_root=raw_root,
                interim_root=interim_root,
                target_rows=12,
            )

            self.assertTrue(artifact.output_path.exists())
            self.assertEqual(artifact.record_count, 12)
            payload = json.loads(artifact.output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["record_count"], 12)
            self.assertEqual(payload["target_record_count"], 12)
            self.assertEqual(len(payload["records"]), 12)

    @staticmethod
    def _sample_payloads() -> tuple[list[dict], list[dict], list[dict]]:
        carts = [
            {
                "id": 1,
                "userId": 10,
                "products": [
                    {
                        "id": 100,
                        "title": "Phone",
                        "price": 100.0,
                        "quantity": 2,
                        "total": 200.0,
                        "discountPercentage": 10.0,
                        "discountedTotal": 180.0,
                    }
                ],
            },
            {
                "id": 2,
                "userId": 11,
                "products": [
                    {
                        "id": 101,
                        "title": "Headphones",
                        "price": 25.0,
                        "quantity": 1,
                        "total": 25.0,
                        "discountPercentage": 5.0,
                        "discountedTotal": 23.75,
                    }
                ],
            },
        ]
        users = [
            {
                "id": 10,
                "firstName": "Ava",
                "lastName": "Stone",
                "email": "ava@example.com",
                "address": {"city": "Dhaka"},
            },
            {
                "id": 11,
                "firstName": "Liam",
                "lastName": "Ray",
                "email": "liam@example.com",
                "address": {"city": "Chattogram"},
            },
        ]
        products = [
            {"id": 100, "title": "Phone", "category": "electronics", "price": 100.0},
            {"id": 101, "title": "Headphones", "category": "audio", "price": 25.0},
        ]
        return carts, users, products

    @staticmethod
    def _write_raw_snapshot(path: Path, resource: str, records: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "resource": resource,
                    "records": records,
                }
            ),
            encoding="utf-8",
        )
