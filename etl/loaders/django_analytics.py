"""Load processed analytics datasets into PostgreSQL with Django ORM."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from django.db import transaction

from apps.analytics.models import CustomerAnalytics, OrderAnalytics
from etl.orchestration import run_dummyjson_analytics_transformation


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AnalyticsLoadArtifact:
    """Metadata describing a completed analytics load."""

    customer_rows_loaded: int
    order_rows_loaded: int
    customer_table_count: int
    order_table_count: int
    customer_output_path: Path
    order_output_path: Path


def load_dummyjson_analytics(
    source: str = "auto",
    as_of_date: date | str | None = None,
    processed_root: Path | None = None,
    orders_root: Path | None = None,
    batch_size: int = 1000,
) -> AnalyticsLoadArtifact:
    """Transform analytics datasets and upsert them into PostgreSQL."""
    analytics_artifact = run_dummyjson_analytics_transformation(
        processed_root=processed_root,
        orders_root=orders_root,
        source=source,
        as_of_date=as_of_date,
    )

    customer_records = _load_records(analytics_artifact.customer_output_path)
    order_records = _load_records(analytics_artifact.order_output_path)

    customer_objects = [_customer_object_from_record(record) for record in customer_records]
    order_objects = [_order_object_from_record(record) for record in order_records]

    with transaction.atomic():
        CustomerAnalytics.objects.bulk_create(
            customer_objects,
            batch_size=batch_size,
            update_conflicts=True,
            update_fields=[
                "full_name",
                "email",
                "email_domain",
                "city",
                "customer_tenure_days",
                "total_orders",
                "total_spent",
                "avg_order_value",
                "lifetime_value_score",
                "customer_segment",
            ],
            unique_fields=["customer_id"],
        )
        OrderAnalytics.objects.bulk_create(
            order_objects,
            batch_size=batch_size,
            update_conflicts=True,
            update_fields=[
                "customer",
                "order_date",
                "order_hour",
                "total_items",
                "gross_amount",
                "total_discount_amount",
                "net_amount",
                "final_amount",
                "discount_ratio",
                "order_complexity_score",
                "dominant_category",
            ],
            unique_fields=["order_id"],
        )

    customer_table_count = CustomerAnalytics.objects.count()
    order_table_count = OrderAnalytics.objects.count()

    LOGGER.info(
        "Loaded %s customer analytics rows and %s order analytics rows into PostgreSQL.",
        len(customer_objects),
        len(order_objects),
    )
    LOGGER.info(
        "Current database counts via ORM: customer_analytics=%s, order_analytics=%s",
        customer_table_count,
        order_table_count,
    )

    return AnalyticsLoadArtifact(
        customer_rows_loaded=len(customer_objects),
        order_rows_loaded=len(order_objects),
        customer_table_count=customer_table_count,
        order_table_count=order_table_count,
        customer_output_path=analytics_artifact.customer_output_path,
        order_output_path=analytics_artifact.order_output_path,
    )


def _load_records(path: Path) -> list[dict[str, Any]]:
    """Load records from a processed analytics JSON file."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"Processed dataset {path} does not contain a 'records' list.")
    return records


def _customer_object_from_record(record: dict[str, Any]) -> CustomerAnalytics:
    """Convert a customer analytics record to a Django model instance."""
    return CustomerAnalytics(
        customer_id=int(record["customer_id"]),
        full_name=record["full_name"],
        email=record["email"],
        email_domain=record["email_domain"],
        city=record.get("city"),
        customer_tenure_days=int(record["customer_tenure_days"]),
        total_orders=int(record["total_orders"]),
        total_spent=Decimal(str(record["total_spent"])),
        avg_order_value=Decimal(str(record["avg_order_value"])),
        lifetime_value_score=Decimal(str(record["lifetime_value_score"])),
        customer_segment=record["customer_segment"],
    )


def _order_object_from_record(record: dict[str, Any]) -> OrderAnalytics:
    """Convert an order analytics record to a Django model instance."""
    return OrderAnalytics(
        order_id=int(record["order_id"]),
        customer_id=int(record["customer_id"]),
        order_date=record["order_date"],
        order_hour=int(record["order_hour"]),
        total_items=int(record["total_items"]),
        gross_amount=Decimal(str(record["gross_amount"])),
        total_discount_amount=Decimal(str(record["total_discount_amount"])),
        net_amount=Decimal(str(record["net_amount"])),
        final_amount=Decimal(str(record["final_amount"])),
        discount_ratio=Decimal(str(record["discount_ratio"])),
        order_complexity_score=int(record["order_complexity_score"]),
        dominant_category=record["dominant_category"],
    )
