"""Generate a deterministic 10000-row synthetic order dataset with pandas."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from etl.transformers.dummyjson_enrichment import (
    enrich_orders_frame,
    load_latest_raw_resource,
    make_order_timestamp,
    make_payment_details,
    make_shipping_cost,
    signup_gap_days,
    stable_int,
)
from etl.utils.paths import INTERIM_DATA_DIR, RAW_DATA_DIR, ensure_directory


LOGGER = logging.getLogger(__name__)


class DummyJSONSyntheticOrdersError(RuntimeError):
    """Raised when synthetic order generation cannot complete safely."""


@dataclass(frozen=True, slots=True)
class SyntheticOrdersArtifact:
    """Metadata describing a saved synthetic orders snapshot."""

    output_path: Path
    record_count: int
    base_record_count: int
    target_record_count: int
    generated_at: str
    source_snapshot_paths: dict[str, Path]


def generate_synthetic_orders_frame(
    carts: list[dict[str, Any]],
    users: list[dict[str, Any]],
    products: list[dict[str, Any]],
    target_rows: int = 10000,
) -> pd.DataFrame:
    """Scale the base enriched order dataset into a deterministic synthetic dataset."""
    if target_rows <= 0:
        raise ValueError("target_rows must be greater than zero.")

    base_orders_df = enrich_orders_frame(carts=carts, users=users, products=products).copy()
    if base_orders_df.empty:
        raise DummyJSONSyntheticOrdersError("The base enriched order dataset is empty.")

    customer_fields_df = pd.json_normalize(base_orders_df["customer"]).rename(
        columns={
            "id": "customer_id",
            "name": "customer_name",
            "email": "customer_email",
            "city": "customer_city",
            "signup_date": "base_signup_date",
        }
    )
    base_orders_df = pd.concat([base_orders_df.reset_index(drop=True), customer_fields_df], axis=1)
    base_orders_df["template_order_id"] = pd.to_numeric(
        base_orders_df["order_id"],
        errors="raise",
    ).astype("int64")
    base_orders_df["customer_id"] = pd.to_numeric(
        base_orders_df["customer_id"],
        errors="raise",
    ).astype("int64")

    repeats = (target_rows + len(base_orders_df) - 1) // len(base_orders_df)
    synthetic_headers_df = (
        base_orders_df.loc[base_orders_df.index.repeat(repeats)]
        .head(target_rows)
        .reset_index(drop=True)
        .copy()
    )
    synthetic_headers_df["synthetic_sequence"] = range(target_rows)
    synthetic_headers_df["replica_index"] = synthetic_headers_df.groupby(
        "template_order_id",
        sort=False,
    ).cumcount()
    synthetic_headers_df["order_id"] = 1_000_000 + synthetic_headers_df["synthetic_sequence"] + 1
    synthetic_headers_df["order_id"] = synthetic_headers_df["order_id"].astype("int64")
    synthetic_headers_df["order_timestamp_dt"] = pd.to_datetime(
        [
            make_order_timestamp(int(order_id), int(customer_id))
            for order_id, customer_id in zip(
                synthetic_headers_df["order_id"],
                synthetic_headers_df["customer_id"],
            )
        ],
        utc=True,
    )

    synthetic_lines_df = _build_synthetic_order_lines_frame(synthetic_headers_df)
    order_metrics_df = _build_synthetic_order_metrics_frame(synthetic_lines_df)
    product_lists_df = _build_synthetic_product_lists_frame(synthetic_lines_df)

    synthetic_orders_df = (
        synthetic_headers_df[
            [
                "synthetic_sequence",
                "order_id",
                "customer_id",
                "customer_name",
                "customer_email",
                "customer_city",
                "order_timestamp_dt",
            ]
        ]
        .drop_duplicates(subset=["order_id"])
        .merge(order_metrics_df, on=["order_id", "customer_id"], how="inner", validate="one_to_one")
        .merge(product_lists_df, on="order_id", how="inner", validate="one_to_one")
    )

    first_orders_df = (
        synthetic_orders_df.groupby("customer_id", as_index=False)["order_timestamp_dt"]
        .min()
        .rename(columns={"order_timestamp_dt": "first_order_timestamp"})
    )
    first_orders_df["signup_gap_days"] = first_orders_df["customer_id"].map(signup_gap_days)
    first_orders_df["signup_date"] = (
        first_orders_df["first_order_timestamp"]
        - pd.to_timedelta(first_orders_df["signup_gap_days"], unit="D")
    ).dt.strftime("%Y-%m-%d")

    synthetic_orders_df = synthetic_orders_df.merge(
        first_orders_df[["customer_id", "signup_date"]],
        on="customer_id",
        how="left",
        validate="many_to_one",
    )
    synthetic_orders_df["payment_details"] = synthetic_orders_df.apply(
        lambda row: make_payment_details(
            order_id=int(row["order_id"]),
            customer_id=int(row["customer_id"]),
        ),
        axis=1,
    )
    synthetic_orders_df["customer"] = synthetic_orders_df.apply(
        lambda row: {
            "id": int(row["customer_id"]),
            "name": row["customer_name"],
            "email": row["customer_email"],
            "city": row["customer_city"],
            "signup_date": row["signup_date"],
        },
        axis=1,
    )
    synthetic_orders_df["order_timestamp"] = synthetic_orders_df["order_timestamp_dt"].dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    return synthetic_orders_df[
        [
            "synthetic_sequence",
            "order_id",
            "order_timestamp",
            "customer",
            "products",
            "payment_details",
            "shipping_cost",
            "total_items",
            "gross_amount",
            "total_discount_amount",
            "net_amount",
        ]
    ].sort_values("synthetic_sequence")


def generate_synthetic_orders(
    carts: list[dict[str, Any]],
    users: list[dict[str, Any]],
    products: list[dict[str, Any]],
    target_rows: int = 10000,
) -> list[dict[str, Any]]:
    """Return the synthetic order dataset as JSON-serializable Python records."""
    synthetic_orders_df = generate_synthetic_orders_frame(
        carts=carts,
        users=users,
        products=products,
        target_rows=target_rows,
    )

    records: list[dict[str, Any]] = []
    for row in synthetic_orders_df.itertuples(index=False):
        records.append(
            {
                "order_id": int(row.order_id),
                "order_timestamp": row.order_timestamp,
                "customer": row.customer,
                "products": row.products,
                "payment_details": row.payment_details,
                "shipping_cost": float(row.shipping_cost),
                "total_items": int(row.total_items),
                "gross_amount": float(row.gross_amount),
                "total_discount_amount": float(row.total_discount_amount),
                "net_amount": float(row.net_amount),
            }
        )
    return records


def build_synthetic_orders_dataset(
    raw_root: Path = RAW_DATA_DIR,
    interim_root: Path = INTERIM_DATA_DIR,
    target_rows: int = 10000,
) -> SyntheticOrdersArtifact:
    """Build and persist a synthetic order dataset from the latest raw snapshots."""
    carts, carts_path = load_latest_raw_resource("carts", raw_root=raw_root)
    users, users_path = load_latest_raw_resource("users", raw_root=raw_root)
    products, products_path = load_latest_raw_resource("products", raw_root=raw_root)

    base_record_count = len(carts)
    synthetic_orders = generate_synthetic_orders(
        carts=carts,
        users=users,
        products=products,
        target_rows=target_rows,
    )

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    safe_timestamp = generated_at.replace("-", "").replace(":", "")
    output_directory = ensure_directory(interim_root / "orders")
    output_path = output_directory / f"dummyjson_synthetic_orders_{target_rows}_{safe_timestamp}.json"

    payload = {
        "resource": "synthetic_orders",
        "source": "DummyJSON",
        "generated_at": generated_at,
        "base_record_count": base_record_count,
        "target_record_count": target_rows,
        "record_count": len(synthetic_orders),
        "source_snapshot_paths": {
            "carts": str(carts_path),
            "users": str(users_path),
            "products": str(products_path),
        },
        "generation_strategy": {
            "template_source": "latest enriched orders derived from raw DummyJSON snapshots",
            "quantity_variation": "deterministic per order and product",
            "discount_variation": "deterministic per order and product",
            "timestamp_strategy": "deterministic synthetic order timestamp per generated order",
        },
        "records": synthetic_orders,
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOGGER.info(
        "Saved %s synthetic orders to %s using %s base orders.",
        len(synthetic_orders),
        output_path,
        base_record_count,
    )

    return SyntheticOrdersArtifact(
        output_path=output_path,
        record_count=len(synthetic_orders),
        base_record_count=base_record_count,
        target_record_count=target_rows,
        generated_at=generated_at,
        source_snapshot_paths={
            "carts": carts_path,
            "users": users_path,
            "products": products_path,
        },
    )


def _build_synthetic_order_lines_frame(synthetic_headers_df: pd.DataFrame) -> pd.DataFrame:
    """Explode generated order headers into synthetic line items."""
    exploded_df = synthetic_headers_df[
        ["order_id", "template_order_id", "replica_index", "customer_id", "products"]
    ].explode("products", ignore_index=True)

    if exploded_df["products"].isna().any():
        raise DummyJSONSyntheticOrdersError(
            "One or more generated order templates do not contain product rows."
        )

    exploded_df["line_sequence"] = exploded_df.groupby("order_id", sort=False).cumcount()
    product_items_df = pd.json_normalize(exploded_df.pop("products")).rename(
        columns={
            "id": "product_id",
            "title": "product_title",
            "category": "category",
            "price": "base_price",
            "quantity": "base_quantity",
            "discount": "base_discount",
        }
    )
    lines_df = pd.concat([exploded_df, product_items_df], axis=1)

    required_columns = {"product_id", "base_price", "base_quantity", "base_discount"}
    missing_columns = required_columns - set(lines_df.columns)
    if missing_columns:
        raise DummyJSONSyntheticOrdersError(
            f"Synthetic line items are missing required fields: {sorted(missing_columns)}."
        )

    lines_df["product_id"] = pd.to_numeric(lines_df["product_id"], errors="raise").astype("int64")
    lines_df["base_price"] = pd.to_numeric(lines_df["base_price"], errors="raise").round(2)
    lines_df["base_quantity"] = pd.to_numeric(lines_df["base_quantity"], errors="raise").astype("int64")
    lines_df["base_discount"] = pd.to_numeric(lines_df["base_discount"], errors="raise").round(2)

    lines_df["quantity_adjustment"] = [
        (stable_int("synthetic_qty", int(order_id), int(product_id), int(replica_index)) % 3) - 1
        for order_id, product_id, replica_index in zip(
            lines_df["order_id"],
            lines_df["product_id"],
            lines_df["replica_index"],
        )
    ]
    lines_df["discount_adjustment"] = [
        ((stable_int("synthetic_discount", int(order_id), int(product_id), int(replica_index)) % 5) - 2)
        * 0.25
        for order_id, product_id, replica_index in zip(
            lines_df["order_id"],
            lines_df["product_id"],
            lines_df["replica_index"],
        )
    ]

    lines_df["quantity"] = (
        lines_df["base_quantity"] + lines_df["quantity_adjustment"]
    ).clip(lower=1).astype("int64")
    lines_df["price"] = lines_df["base_price"].round(2)
    lines_df["discount"] = (
        lines_df["base_discount"] + lines_df["discount_adjustment"]
    ).clip(lower=0.0, upper=35.0).round(2)
    lines_df["gross_line_amount"] = (lines_df["price"] * lines_df["quantity"]).round(2)
    lines_df["discount_amount"] = (
        lines_df["gross_line_amount"] * (lines_df["discount"] / 100.0)
    ).round(2)
    lines_df["net_line_amount"] = (
        lines_df["gross_line_amount"] - lines_df["discount_amount"]
    ).round(2)

    return lines_df[
        [
            "order_id",
            "customer_id",
            "line_sequence",
            "product_id",
            "product_title",
            "category",
            "price",
            "quantity",
            "discount",
            "gross_line_amount",
            "discount_amount",
            "net_line_amount",
        ]
    ]


def _build_synthetic_order_metrics_frame(synthetic_lines_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate synthetic line-item metrics back to the order level."""
    metrics_df = (
        synthetic_lines_df.groupby(["order_id", "customer_id"], as_index=False, sort=False)
        .agg(
            total_items=("quantity", "sum"),
            gross_amount=("gross_line_amount", "sum"),
            total_discount_amount=("discount_amount", "sum"),
        )
        .copy()
    )
    metrics_df["gross_amount"] = metrics_df["gross_amount"].round(2)
    metrics_df["total_discount_amount"] = metrics_df["total_discount_amount"].round(2)
    metrics_df["net_amount"] = (
        metrics_df["gross_amount"] - metrics_df["total_discount_amount"]
    ).round(2)
    metrics_df["shipping_cost"] = metrics_df.apply(
        lambda row: make_shipping_cost(
            total_items=int(row["total_items"]),
            gross_amount=float(row["gross_amount"]),
        ),
        axis=1,
    )
    return metrics_df


def _build_synthetic_product_lists_frame(synthetic_lines_df: pd.DataFrame) -> pd.DataFrame:
    """Build nested product arrays per synthetic order."""
    return (
        synthetic_lines_df.sort_values(["order_id", "line_sequence"])
        .groupby("order_id", sort=False)[
            ["product_id", "product_title", "category", "price", "quantity", "discount"]
        ]
        .apply(_product_records_from_group)
        .rename("products")
        .reset_index()
    )


def _product_records_from_group(group: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a grouped synthetic line-item DataFrame into nested product records."""
    records: list[dict[str, Any]] = []
    for row in group.itertuples(index=False):
        records.append(
            {
                "id": int(row.product_id),
                "title": row.product_title,
                "category": row.category,
                "price": float(row.price),
                "quantity": int(row.quantity),
                "discount": float(row.discount),
            }
        )
    return records
