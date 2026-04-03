"""Deterministic synthetic enrichment for DummyJSON order data using pandas."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from etl.utils.paths import INTERIM_DATA_DIR, RAW_DATA_DIR, ensure_directory


LOGGER = logging.getLogger(__name__)
PAYMENT_METHODS = ("card", "paypal", "bank_transfer", "cash_on_delivery")
ORDER_TIMESTAMP_ANCHOR = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


class DummyJSONEnrichmentError(RuntimeError):
    """Raised when raw DummyJSON data cannot be enriched safely."""


@dataclass(frozen=True, slots=True)
class EnrichmentArtifact:
    """Metadata describing a saved enriched snapshot."""

    output_path: Path
    record_count: int
    generated_at: str
    source_snapshot_paths: dict[str, Path]


def stable_int(*parts: Any) -> int:
    """Build a deterministic integer from a set of input parts."""
    raw = "|".join(map(str, parts)).encode("utf-8")
    return int(hashlib.md5(raw).hexdigest()[:8], 16)


def make_order_timestamp(cart_id: int, user_id: int) -> datetime:
    """Create a deterministic synthetic order timestamp."""
    seed = stable_int("order_ts", cart_id, user_id)
    return ORDER_TIMESTAMP_ANCHOR + timedelta(
        days=seed % 365,
        hours=(seed // 365) % 24,
        minutes=(seed // 1000) % 60,
    )


def signup_gap_days(customer_id: int) -> int:
    """Return a deterministic number of days between signup and first order."""
    return 30 + (stable_int("signup", customer_id) % 720)


def make_payment_details(order_id: int, customer_id: int) -> dict[str, str]:
    """Create deterministic synthetic payment details for an order."""
    seed = stable_int("payment", order_id, customer_id)
    transaction_id = stable_int("txn", order_id) % 100_000_000
    return {
        "payment_method": PAYMENT_METHODS[seed % len(PAYMENT_METHODS)],
        "payment_status": "paid",
        "transaction_id": f"TXN-{transaction_id:08d}",
    }


def make_shipping_cost(total_items: int, gross_amount: float) -> float:
    """Create a simple deterministic shipping cost from basket size and value."""
    if gross_amount >= 300:
        return 0.0
    return round(4.99 + (0.50 * total_items), 2)


def load_latest_raw_resource(
    resource_name: str,
    raw_root: Path = RAW_DATA_DIR,
) -> tuple[list[dict[str, Any]], Path]:
    """Load the latest raw snapshot for a resource."""
    resource_directory = raw_root / resource_name
    candidates = sorted(resource_directory.glob("*.json"))

    if not candidates:
        raise DummyJSONEnrichmentError(
            f"No raw snapshots were found for '{resource_name}' in {resource_directory}."
        )

    latest_path = candidates[-1]
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    records = payload.get("records")

    if not isinstance(records, list):
        raise DummyJSONEnrichmentError(
            f"Raw snapshot {latest_path} does not contain a 'records' list."
        )

    return records, latest_path


def enrich_orders_frame(
    carts: list[dict[str, Any]],
    users: list[dict[str, Any]],
    products: list[dict[str, Any]],
) -> pd.DataFrame:
    """Build the enriched order dataset as a pandas DataFrame."""
    orders_df = _build_orders_frame(carts)
    users_df = _build_users_frame(users)
    product_lines_df = _build_order_lines_frame(carts, products)
    order_metrics_df = _build_order_metrics_frame(product_lines_df)
    product_lists_df = _build_product_lists_frame(product_lines_df)

    enriched_df = (
        orders_df.merge(users_df, on="customer_id", how="left", validate="many_to_one")
        .merge(order_metrics_df, on=["order_id", "customer_id"], how="inner", validate="one_to_one")
        .merge(product_lists_df, on="order_id", how="inner", validate="one_to_one")
    )

    missing_user_ids = (
        enriched_df.loc[enriched_df["customer_email"].isna(), "customer_id"].drop_duplicates().tolist()
    )
    if missing_user_ids:
        raise DummyJSONEnrichmentError(
            f"Orders reference users that do not exist in the raw user snapshot: {missing_user_ids}."
        )

    first_orders_df = (
        enriched_df.groupby("customer_id", as_index=False)["order_timestamp"].min()
        .rename(columns={"order_timestamp": "first_order_timestamp"})
    )
    first_orders_df["signup_gap_days"] = first_orders_df["customer_id"].map(signup_gap_days)
    first_orders_df["signup_date"] = (
        first_orders_df["first_order_timestamp"]
        - pd.to_timedelta(first_orders_df["signup_gap_days"], unit="D")
    ).dt.strftime("%Y-%m-%d")

    enriched_df = enriched_df.merge(
        first_orders_df[["customer_id", "signup_date"]],
        on="customer_id",
        how="left",
        validate="many_to_one",
    )

    enriched_df["payment_details"] = enriched_df.apply(
        lambda row: make_payment_details(
            order_id=int(row["order_id"]),
            customer_id=int(row["customer_id"]),
        ),
        axis=1,
    )
    enriched_df["customer"] = enriched_df.apply(
        lambda row: {
            "id": int(row["customer_id"]),
            "name": row["customer_name"],
            "email": row["customer_email"],
            "city": row["customer_city"],
            "signup_date": row["signup_date"],
        },
        axis=1,
    )
    enriched_df["order_timestamp"] = enriched_df["order_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return enriched_df[
        [
            "order_sequence",
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
    ].sort_values("order_sequence")


def enrich_orders(
    carts: list[dict[str, Any]],
    users: list[dict[str, Any]],
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return the enriched order dataset as JSON-serializable Python records."""
    enriched_df = enrich_orders_frame(carts=carts, users=users, products=products)

    records: list[dict[str, Any]] = []
    for row in enriched_df.itertuples(index=False):
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


def build_enriched_orders_dataset(
    raw_root: Path = RAW_DATA_DIR,
    interim_root: Path = INTERIM_DATA_DIR,
) -> EnrichmentArtifact:
    """Build and persist an enriched order dataset from the latest raw snapshots."""
    carts, carts_path = load_latest_raw_resource("carts", raw_root=raw_root)
    users, users_path = load_latest_raw_resource("users", raw_root=raw_root)
    products, products_path = load_latest_raw_resource("products", raw_root=raw_root)

    enriched_orders = enrich_orders(carts=carts, users=users, products=products)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    safe_timestamp = generated_at.replace("-", "").replace(":", "")
    output_directory = ensure_directory(interim_root / "orders")
    output_path = output_directory / f"dummyjson_enriched_orders_{safe_timestamp}.json"

    payload = {
        "resource": "enriched_orders",
        "source": "DummyJSON",
        "generated_at": generated_at,
        "record_count": len(enriched_orders),
        "source_snapshot_paths": {
            "carts": str(carts_path),
            "users": str(users_path),
            "products": str(products_path),
        },
        "records": enriched_orders,
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOGGER.info("Saved %s enriched orders to %s", len(enriched_orders), output_path)

    return EnrichmentArtifact(
        output_path=output_path,
        record_count=len(enriched_orders),
        generated_at=generated_at,
        source_snapshot_paths={
            "carts": carts_path,
            "users": users_path,
            "products": products_path,
        },
    )


def _build_orders_frame(carts: list[dict[str, Any]]) -> pd.DataFrame:
    """Build the order-level DataFrame from raw cart records."""
    carts_df = pd.DataFrame(carts)
    if carts_df.empty:
        raise DummyJSONEnrichmentError("No cart records were provided for enrichment.")

    required_columns = {"id", "userId"}
    missing_columns = required_columns - set(carts_df.columns)
    if missing_columns:
        raise DummyJSONEnrichmentError(
            f"Cart records are missing required fields: {sorted(missing_columns)}."
        )

    orders_df = carts_df.loc[:, ["id", "userId"]].drop_duplicates().rename(
        columns={"id": "order_id", "userId": "customer_id"}
    )
    orders_df["order_id"] = pd.to_numeric(orders_df["order_id"], errors="raise").astype("int64")
    orders_df["customer_id"] = pd.to_numeric(orders_df["customer_id"], errors="raise").astype("int64")
    orders_df["order_sequence"] = range(len(orders_df))
    orders_df["order_timestamp"] = pd.to_datetime(
        orders_df.apply(
            lambda row: make_order_timestamp(
                cart_id=int(row["order_id"]),
                user_id=int(row["customer_id"]),
            ),
            axis=1,
        ),
        utc=True,
    )
    return orders_df


def _build_users_frame(users: list[dict[str, Any]]) -> pd.DataFrame:
    """Build the customer dimension DataFrame from raw users."""
    users_df = pd.json_normalize(users)
    if users_df.empty:
        raise DummyJSONEnrichmentError("No user records were provided for enrichment.")

    required_columns = {"id", "firstName", "lastName", "email"}
    missing_columns = required_columns - set(users_df.columns)
    if missing_columns:
        raise DummyJSONEnrichmentError(
            f"User records are missing required fields: {sorted(missing_columns)}."
        )

    first_name = users_df["firstName"].fillna("").astype(str).str.strip()
    last_name = users_df["lastName"].fillna("").astype(str).str.strip()
    customer_name = (first_name + " " + last_name).str.split().str.join(" ")
    customer_city = _column_or_default(users_df, "address.city", None)

    return pd.DataFrame(
        {
            "customer_id": pd.to_numeric(users_df["id"], errors="raise").astype("int64"),
            "customer_name": customer_name,
            "customer_email": users_df["email"].fillna("").astype(str).str.lower(),
            "customer_city": customer_city.where(customer_city.notna(), None),
        }
    )


def _build_order_lines_frame(
    carts: list[dict[str, Any]],
    products: list[dict[str, Any]],
) -> pd.DataFrame:
    """Explode cart products and join them to the product dimension."""
    carts_df = pd.DataFrame(carts)
    if carts_df.empty:
        raise DummyJSONEnrichmentError("No cart records were provided for enrichment.")
    if "products" not in carts_df.columns:
        raise DummyJSONEnrichmentError("Cart records do not contain a 'products' field.")

    exploded_df = (
        carts_df.loc[:, ["id", "userId", "products"]]
        .rename(columns={"id": "order_id", "userId": "customer_id"})
        .explode("products", ignore_index=True)
    )
    if exploded_df["products"].isna().any():
        raise DummyJSONEnrichmentError("One or more carts do not contain product rows.")

    exploded_df["line_sequence"] = exploded_df.groupby("order_id", sort=False).cumcount()
    product_items_df = pd.json_normalize(exploded_df.pop("products"))

    lines_df = pd.concat([exploded_df, product_items_df], axis=1).rename(
        columns={
            "id": "product_id",
            "title": "cart_product_title",
            "price": "cart_price",
            "quantity": "quantity",
            "total": "cart_line_total",
            "discountPercentage": "cart_discount_percentage",
            "discountedTotal": "cart_discounted_total",
        }
    )

    required_line_columns = {"product_id", "quantity"}
    missing_line_columns = required_line_columns - set(lines_df.columns)
    if missing_line_columns:
        raise DummyJSONEnrichmentError(
            f"Cart product rows are missing required fields: {sorted(missing_line_columns)}."
        )

    products_df = pd.json_normalize(products)
    if products_df.empty:
        raise DummyJSONEnrichmentError("No product records were provided for enrichment.")

    required_product_columns = {"id", "title", "category", "price"}
    missing_product_columns = required_product_columns - set(products_df.columns)
    if missing_product_columns:
        raise DummyJSONEnrichmentError(
            f"Product records are missing required fields: {sorted(missing_product_columns)}."
        )

    product_dimension_df = pd.DataFrame(
        {
            "product_id": pd.to_numeric(products_df["id"], errors="raise").astype("int64"),
            "product_master_title": products_df["title"],
            "category": products_df["category"],
            "product_master_price": pd.to_numeric(products_df["price"], errors="raise"),
            "product_master_discount_percentage": pd.to_numeric(
                _column_or_default(products_df, "discountPercentage", 0.0),
                errors="coerce",
            ).fillna(0.0),
        }
    )

    lines_df["order_id"] = pd.to_numeric(lines_df["order_id"], errors="raise").astype("int64")
    lines_df["customer_id"] = pd.to_numeric(lines_df["customer_id"], errors="raise").astype("int64")
    lines_df["product_id"] = pd.to_numeric(lines_df["product_id"], errors="raise").astype("int64")
    lines_df["quantity"] = pd.to_numeric(lines_df["quantity"], errors="raise").astype("int64")

    lines_df = lines_df.merge(
        product_dimension_df,
        on="product_id",
        how="left",
        validate="many_to_one",
    )

    missing_product_ids = (
        lines_df.loc[lines_df["category"].isna(), "product_id"].drop_duplicates().tolist()
    )
    if missing_product_ids:
        raise DummyJSONEnrichmentError(
            f"Cart rows reference products that do not exist in the raw product snapshot: {missing_product_ids}."
        )

    cart_price = pd.to_numeric(_column_or_default(lines_df, "cart_price", pd.NA), errors="coerce")
    cart_line_total = pd.to_numeric(
        _column_or_default(lines_df, "cart_line_total", pd.NA),
        errors="coerce",
    )
    cart_discount_percentage = pd.to_numeric(
        _column_or_default(lines_df, "cart_discount_percentage", pd.NA),
        errors="coerce",
    )
    cart_discounted_total = pd.to_numeric(
        _column_or_default(lines_df, "cart_discounted_total", pd.NA),
        errors="coerce",
    )

    lines_df["product_title"] = _column_or_default(lines_df, "cart_product_title", pd.NA).fillna(
        lines_df["product_master_title"]
    )
    lines_df["price"] = cart_price.fillna(lines_df["product_master_price"]).round(2)
    lines_df["discount"] = (
        cart_discount_percentage.fillna(lines_df["product_master_discount_percentage"]).fillna(0.0).round(2)
    )
    lines_df["gross_line_amount"] = cart_line_total.fillna(lines_df["price"] * lines_df["quantity"]).round(2)
    calculated_net_amount = (
        lines_df["gross_line_amount"] * (1 - (lines_df["discount"] / 100.0))
    ).round(2)
    lines_df["net_line_amount"] = cart_discounted_total.fillna(calculated_net_amount).round(2)
    lines_df["discount_amount"] = (lines_df["gross_line_amount"] - lines_df["net_line_amount"]).round(2)

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
            "net_line_amount",
            "discount_amount",
        ]
    ]


def _build_order_metrics_frame(product_lines_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate line-item metrics back to the order level."""
    metrics_df = (
        product_lines_df.groupby(["order_id", "customer_id"], as_index=False, sort=False)
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


def _build_product_lists_frame(product_lines_df: pd.DataFrame) -> pd.DataFrame:
    """Build nested product arrays per order using pandas grouping."""
    product_lists = (
        product_lines_df.sort_values(["order_id", "line_sequence"])
        .groupby("order_id", sort=False)[
            ["product_id", "product_title", "category", "price", "quantity", "discount"]
        ]
        .apply(_product_records_from_group)
        .rename("products")
        .reset_index()
    )
    return product_lists


def _product_records_from_group(group: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a grouped line-item DataFrame into nested product records."""
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


def _column_or_default(
    frame: pd.DataFrame,
    column_name: str,
    default_value: Any,
) -> pd.Series:
    """Return a DataFrame column or a same-length default series."""
    if column_name in frame.columns:
        return frame[column_name]
    return pd.Series([default_value] * len(frame), index=frame.index)
