"""Unit tests for the DummyJSON analytics transformation layer."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from etl.transformers.dummyjson_analytics import (
    build_analytics_datasets,
    build_analytics_frames,
    build_analytics_tables,
    load_latest_order_dataset,
)


class DummyJSONAnalyticsTests(unittest.TestCase):
    """Validate customer_analytics and order_analytics calculations."""

    def test_build_analytics_frames_computes_expected_metrics(self) -> None:
        orders = self._sample_orders()

        customer_df, order_df = build_analytics_frames(
            orders=orders,
            as_of_date="2026-04-03",
        )

        self.assertIsInstance(customer_df, pd.DataFrame)
        self.assertIsInstance(order_df, pd.DataFrame)
        self.assertEqual(len(customer_df), 2)
        self.assertEqual(len(order_df), 3)

        order_1001 = order_df.loc[order_df["order_id"] == 1001].iloc[0]
        self.assertEqual(order_1001["order_date"], "2025-04-01")
        self.assertEqual(order_1001["order_hour"], 10)
        self.assertEqual(order_1001["total_items"], 3)
        self.assertAlmostEqual(order_1001["gross_amount"], 160.0, places=2)
        self.assertAlmostEqual(order_1001["total_discount_amount"], 13.0, places=2)
        self.assertAlmostEqual(order_1001["net_amount"], 147.0, places=2)
        self.assertAlmostEqual(order_1001["final_amount"], 153.99, places=2)
        self.assertAlmostEqual(order_1001["discount_ratio"], 0.0812, places=4)
        self.assertEqual(order_1001["order_complexity_score"], 7)
        self.assertEqual(order_1001["dominant_category"], "electronics")

        customer_10 = customer_df.loc[customer_df["customer_id"] == 10].iloc[0]
        self.assertEqual(customer_10["full_name"], "Ava Stone")
        self.assertEqual(customer_10["email"], "ava@example.com")
        self.assertEqual(customer_10["email_domain"], "example.com")
        self.assertEqual(customer_10["customer_tenure_days"], 426)
        self.assertEqual(customer_10["total_orders"], 2)
        self.assertAlmostEqual(customer_10["total_spent"], 201.92, places=2)
        self.assertAlmostEqual(customer_10["avg_order_value"], 100.96, places=2)
        self.assertIn(customer_10["customer_segment"], {"High", "Medium", "Low"})

    def test_load_latest_order_dataset_prefers_synthetic_in_auto_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            orders_root = Path(temp_dir)
            self._write_orders_snapshot(
                orders_root / "dummyjson_enriched_orders_20260403T000000Z.json",
                resource="enriched_orders",
                records=[{"order_id": 1, "customer": {}, "products": []}],
            )
            self._write_orders_snapshot(
                orders_root / "dummyjson_synthetic_orders_10000_20260403T000100Z.json",
                resource="synthetic_orders",
                records=[{"order_id": 2, "customer": {}, "products": []}],
            )

            records, selected_path, resource = load_latest_order_dataset(
                orders_root=orders_root,
                source="auto",
            )

            self.assertEqual(resource, "synthetic_orders")
            self.assertEqual(records[0]["order_id"], 2)
            self.assertIn("synthetic", selected_path.name)

    def test_build_analytics_datasets_writes_both_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            orders_root = root / "orders"
            processed_root = root / "processed"
            self._write_orders_snapshot(
                orders_root / "dummyjson_synthetic_orders_10000_20260403T000000Z.json",
                resource="synthetic_orders",
                records=self._sample_orders(),
            )

            artifact = build_analytics_datasets(
                processed_root=processed_root,
                orders_root=orders_root,
                source="synthetic",
                as_of_date="2026-04-03",
            )

            self.assertTrue(artifact.customer_output_path.exists())
            self.assertTrue(artifact.order_output_path.exists())

            customer_payload = json.loads(
                artifact.customer_output_path.read_text(encoding="utf-8")
            )
            order_payload = json.loads(artifact.order_output_path.read_text(encoding="utf-8"))

            self.assertEqual(customer_payload["resource"], "customer_analytics")
            self.assertEqual(order_payload["resource"], "order_analytics")
            self.assertEqual(customer_payload["record_count"], 2)
            self.assertEqual(order_payload["record_count"], 3)

    def test_build_analytics_tables_returns_serializable_records(self) -> None:
        customer_records, order_records = build_analytics_tables(
            orders=self._sample_orders(),
            as_of_date="2026-04-03",
        )

        self.assertEqual(len(customer_records), 2)
        self.assertEqual(len(order_records), 3)
        self.assertIsInstance(customer_records[0]["customer_tenure_days"], int)
        self.assertIsInstance(order_records[0]["order_hour"], int)

    @staticmethod
    def _sample_orders() -> list[dict]:
        return [
            {
                "order_id": 1001,
                "order_timestamp": "2025-04-01T10:30:00Z",
                "customer": {
                    "id": 10,
                    "name": "Ava Stone",
                    "email": "ava@example.com",
                    "city": "Dhaka",
                    "signup_date": "2025-02-01",
                },
                "products": [
                    {
                        "id": 1,
                        "title": "Phone",
                        "category": "electronics",
                        "price": 100.0,
                        "quantity": 1,
                        "discount": 10.0,
                    },
                    {
                        "id": 2,
                        "title": "Headphones",
                        "category": "electronics",
                        "price": 30.0,
                        "quantity": 2,
                        "discount": 5.0,
                    },
                ],
                "payment_details": {"payment_method": "card", "payment_status": "paid"},
                "shipping_cost": 6.99,
                "total_items": 3,
                "gross_amount": 160.0,
                "total_discount_amount": 13.0,
                "net_amount": 147.0,
            },
            {
                "order_id": 1002,
                "order_timestamp": "2025-04-02T18:15:00Z",
                "customer": {
                    "id": 10,
                    "name": "Ava Stone",
                    "email": "ava@example.com",
                    "city": "Dhaka",
                    "signup_date": "2025-02-01",
                },
                "products": [
                    {
                        "id": 3,
                        "title": "Notebook",
                        "category": "stationery",
                        "price": 20.0,
                        "quantity": 2,
                        "discount": 0.0,
                    },
                ],
                "payment_details": {"payment_method": "paypal", "payment_status": "paid"},
                "shipping_cost": 7.93,
                "total_items": 2,
                "gross_amount": 40.0,
                "total_discount_amount": 0.0,
                "net_amount": 40.0,
            },
            {
                "order_id": 1003,
                "order_timestamp": "2025-04-03T09:00:00Z",
                "customer": {
                    "id": 11,
                    "name": "Liam Ray",
                    "email": "liam@example.com",
                    "city": "Chattogram",
                    "signup_date": "2025-01-15",
                },
                "products": [
                    {
                        "id": 4,
                        "title": "Keyboard",
                        "category": "electronics",
                        "price": 50.0,
                        "quantity": 1,
                        "discount": 20.0,
                    },
                ],
                "payment_details": {"payment_method": "bank_transfer", "payment_status": "paid"},
                "shipping_cost": 5.0,
                "total_items": 1,
                "gross_amount": 50.0,
                "total_discount_amount": 10.0,
                "net_amount": 40.0,
            },
        ]

    @staticmethod
    def _write_orders_snapshot(path: Path, resource: str, records: list[dict]) -> None:
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
