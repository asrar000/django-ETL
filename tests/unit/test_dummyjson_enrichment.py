"""Unit tests for the DummyJSON synthetic enrichment logic."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

from etl.transformers.dummyjson_enrichment import (
    build_enriched_orders_dataset,
    enrich_orders,
    enrich_orders_frame,
    make_order_timestamp,
    make_payment_details,
    make_shipping_cost,
    signup_gap_days,
)


class DummyJSONEnrichmentTests(unittest.TestCase):
    """Validate deterministic synthetic enrichment behavior."""

    def test_make_order_timestamp_is_deterministic(self) -> None:
        first = make_order_timestamp(cart_id=5, user_id=9)
        second = make_order_timestamp(cart_id=5, user_id=9)
        self.assertEqual(first, second)

    def test_payment_details_and_shipping_are_deterministic(self) -> None:
        self.assertEqual(
            make_payment_details(order_id=11, customer_id=7),
            make_payment_details(order_id=11, customer_id=7),
        )
        self.assertEqual(make_shipping_cost(total_items=3, gross_amount=299.99), 6.49)
        self.assertEqual(make_shipping_cost(total_items=3, gross_amount=300.0), 0.0)

    def test_enrich_orders_adds_synthetic_fields_and_signup_precedes_first_order(self) -> None:
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
                "userId": 10,
                "products": [
                    {
                        "id": 101,
                        "title": "Headphones",
                        "price": 25.0,
                        "quantity": 1,
                        "total": 25.0,
                        "discountPercentage": 0.0,
                        "discountedTotal": 25.0,
                    }
                ],
            },
        ]
        users = [
            {
                "id": 10,
                "firstName": "Ava",
                "lastName": "Stone",
                "email": "AVA@example.com",
                "address": {"city": "Dhaka"},
            }
        ]
        products = [
            {"id": 100, "title": "Phone", "category": "electronics", "price": 100.0},
            {"id": 101, "title": "Headphones", "category": "audio", "price": 25.0},
        ]

        enriched_frame = enrich_orders_frame(carts=carts, users=users, products=products)
        enriched_orders = enrich_orders(carts=carts, users=users, products=products)

        self.assertIsInstance(enriched_frame, pd.DataFrame)
        self.assertEqual(len(enriched_orders), 2)
        self.assertEqual(list(enriched_frame["order_id"]), [1, 2])
        self.assertEqual(enriched_orders[0]["customer"]["email"], "ava@example.com")
        self.assertIn("payment_method", enriched_orders[0]["payment_details"])
        self.assertEqual(enriched_orders[0]["products"][0]["category"], "electronics")

        signup_date = datetime.fromisoformat(enriched_orders[0]["customer"]["signup_date"])
        first_order_timestamp = min(
            datetime.fromisoformat(order["order_timestamp"].replace("Z", "+00:00"))
            for order in enriched_orders
        )
        self.assertLess(signup_date.date().isoformat(), first_order_timestamp.date().isoformat())
        self.assertGreaterEqual(signup_gap_days(10), 30)

    def test_build_enriched_orders_dataset_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_root = root / "raw"
            interim_root = root / "interim"

            self._write_raw_snapshot(
                raw_root / "carts" / "dummyjson_carts_20260403T000000Z.json",
                "carts",
                [{"id": 1, "userId": 10, "products": [{"id": 100, "quantity": 1, "price": 10.0, "total": 10.0, "discountPercentage": 0.0, "discountedTotal": 10.0}]}],
            )
            self._write_raw_snapshot(
                raw_root / "users" / "dummyjson_users_20260403T000000Z.json",
                "users",
                [{"id": 10, "firstName": "Ava", "lastName": "Stone", "email": "ava@example.com", "address": {"city": "Dhaka"}}],
            )
            self._write_raw_snapshot(
                raw_root / "products" / "dummyjson_products_20260403T000000Z.json",
                "products",
                [{"id": 100, "title": "Phone", "category": "electronics", "price": 10.0}],
            )

            artifact = build_enriched_orders_dataset(raw_root=raw_root, interim_root=interim_root)

            self.assertTrue(artifact.output_path.exists())
            payload = json.loads(artifact.output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["record_count"], 1)
            self.assertEqual(payload["records"][0]["order_id"], 1)

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


if __name__ == "__main__":
    unittest.main()
