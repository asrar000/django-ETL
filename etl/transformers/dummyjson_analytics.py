"""Build customer_analytics and order_analytics tables with pandas."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from etl.utils.paths import INTERIM_DATA_DIR, PROCESSED_DATA_DIR, ensure_directory


LOGGER = logging.getLogger(__name__)
OrderSource = Literal["auto", "synthetic", "enriched"]


class DummyJSONAnalyticsError(RuntimeError):
    """Raised when the analytics tables cannot be built safely."""


@dataclass(frozen=True, slots=True)
class AnalyticsArtifact:
    """Metadata describing saved analytics snapshots."""

    customer_output_path: Path
    order_output_path: Path
    customer_record_count: int
    order_record_count: int
    source_dataset_path: Path
    source_resource: str
    as_of_date: str
    generated_at: str


def load_latest_order_dataset(
    orders_root: Path = INTERIM_DATA_DIR / "orders",
    source: OrderSource = "auto",
) -> tuple[list[dict[str, Any]], Path, str]:
    """Load the latest interim order dataset, preferring synthetic data by default."""
    if source not in {"auto", "synthetic", "enriched"}:
        raise ValueError("source must be one of: auto, synthetic, enriched")

    pattern_map = {
        "synthetic": "dummyjson_synthetic_orders_*.json",
        "enriched": "dummyjson_enriched_orders_*.json",
    }

    if source == "auto":
        synthetic_candidates = sorted(orders_root.glob(pattern_map["synthetic"]))
        if synthetic_candidates:
            selected_path = synthetic_candidates[-1]
        else:
            enriched_candidates = sorted(orders_root.glob(pattern_map["enriched"]))
            if not enriched_candidates:
                raise DummyJSONAnalyticsError(
                    f"No interim order datasets were found in {orders_root}."
                )
            selected_path = enriched_candidates[-1]
    else:
        candidates = sorted(orders_root.glob(pattern_map[source]))
        if not candidates:
            raise DummyJSONAnalyticsError(
                f"No {source} order datasets were found in {orders_root}."
            )
        selected_path = candidates[-1]

    payload = json.loads(selected_path.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        raise DummyJSONAnalyticsError(
            f"Interim dataset {selected_path} does not contain a 'records' list."
        )

    resource = str(payload.get("resource", "orders"))
    return records, selected_path, resource


def build_analytics_frames(
    orders: list[dict[str, Any]],
    as_of_date: date | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build customer_analytics and order_analytics DataFrames."""
    normalized_as_of_date = _normalize_as_of_date(as_of_date)
    orders_df = _build_orders_base_frame(orders)
    product_lines_df = _build_product_lines_frame(orders)
    order_analytics_df = _build_order_analytics_frame(orders_df, product_lines_df)
    customer_analytics_df = _build_customer_analytics_frame(
        orders_df=orders_df,
        order_analytics_df=order_analytics_df,
        as_of_date=normalized_as_of_date,
    )
    return customer_analytics_df, order_analytics_df


def build_analytics_tables(
    orders: list[dict[str, Any]],
    as_of_date: date | str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return analytics tables as JSON-serializable Python records."""
    customer_df, order_df = build_analytics_frames(orders=orders, as_of_date=as_of_date)
    return _frame_to_records(customer_df), _frame_to_records(order_df)


def build_analytics_datasets(
    processed_root: Path = PROCESSED_DATA_DIR,
    orders_root: Path = INTERIM_DATA_DIR / "orders",
    source: OrderSource = "auto",
    as_of_date: date | str | None = None,
) -> AnalyticsArtifact:
    """Build and persist the customer_analytics and order_analytics datasets."""
    orders, source_dataset_path, source_resource = load_latest_order_dataset(
        orders_root=orders_root,
        source=source,
    )
    normalized_as_of_date = _normalize_as_of_date(as_of_date)
    customer_records, order_records = build_analytics_tables(
        orders=orders,
        as_of_date=normalized_as_of_date,
    )

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    safe_timestamp = generated_at.replace("-", "").replace(":", "")

    customer_output_directory = ensure_directory(processed_root / "customer_analytics")
    order_output_directory = ensure_directory(processed_root / "order_analytics")

    customer_output_path = (
        customer_output_directory
        / f"dummyjson_customer_analytics_{source_resource}_{safe_timestamp}.json"
    )
    order_output_path = (
        order_output_directory
        / f"dummyjson_order_analytics_{source_resource}_{safe_timestamp}.json"
    )

    customer_payload = {
        "resource": "customer_analytics",
        "source_dataset_resource": source_resource,
        "source_dataset_path": str(source_dataset_path),
        "generated_at": generated_at,
        "as_of_date": normalized_as_of_date.isoformat(),
        "record_count": len(customer_records),
        "records": customer_records,
    }
    order_payload = {
        "resource": "order_analytics",
        "source_dataset_resource": source_resource,
        "source_dataset_path": str(source_dataset_path),
        "generated_at": generated_at,
        "as_of_date": normalized_as_of_date.isoformat(),
        "record_count": len(order_records),
        "records": order_records,
    }

    customer_output_path.write_text(json.dumps(customer_payload, indent=2), encoding="utf-8")
    order_output_path.write_text(json.dumps(order_payload, indent=2), encoding="utf-8")

    LOGGER.info(
        "Saved %s customer analytics rows to %s",
        len(customer_records),
        customer_output_path,
    )
    LOGGER.info(
        "Saved %s order analytics rows to %s",
        len(order_records),
        order_output_path,
    )

    return AnalyticsArtifact(
        customer_output_path=customer_output_path,
        order_output_path=order_output_path,
        customer_record_count=len(customer_records),
        order_record_count=len(order_records),
        source_dataset_path=source_dataset_path,
        source_resource=source_resource,
        as_of_date=normalized_as_of_date.isoformat(),
        generated_at=generated_at,
    )


def _build_orders_base_frame(orders: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize order-level fields into a DataFrame."""
    orders_df = pd.DataFrame(orders)
    if orders_df.empty:
        raise DummyJSONAnalyticsError("No order records were provided for analytics.")

    customer_df = pd.json_normalize(orders_df["customer"]).rename(
        columns={
            "id": "customer_id",
            "name": "full_name",
            "email": "email",
            "city": "city",
            "signup_date": "signup_date",
        }
    )

    combined_df = pd.concat([orders_df.reset_index(drop=True), customer_df], axis=1)
    required_columns = {
        "order_id",
        "order_timestamp",
        "shipping_cost",
        "customer_id",
        "full_name",
        "email",
        "city",
        "signup_date",
        "products",
    }
    missing_columns = required_columns - set(combined_df.columns)
    if missing_columns:
        raise DummyJSONAnalyticsError(
            f"Order records are missing required fields: {sorted(missing_columns)}."
        )

    combined_df["order_id"] = pd.to_numeric(combined_df["order_id"], errors="raise").astype("int64")
    combined_df["customer_id"] = pd.to_numeric(combined_df["customer_id"], errors="raise").astype("int64")
    combined_df["shipping_cost"] = pd.to_numeric(
        combined_df["shipping_cost"],
        errors="raise",
    ).round(2)
    combined_df["order_timestamp"] = pd.to_datetime(
        combined_df["order_timestamp"],
        utc=True,
        errors="raise",
    )
    combined_df["signup_date"] = pd.to_datetime(
        combined_df["signup_date"],
        errors="raise",
    ).dt.date
    combined_df["email"] = combined_df["email"].fillna("").astype(str).str.lower()
    combined_df["full_name"] = combined_df["full_name"].fillna("").astype(str).str.strip()
    combined_df["city"] = combined_df["city"].where(combined_df["city"].notna(), None)

    return combined_df[
        [
            "order_id",
            "order_timestamp",
            "shipping_cost",
            "customer_id",
            "full_name",
            "email",
            "city",
            "signup_date",
            "products",
        ]
    ]


def _build_product_lines_frame(orders: list[dict[str, Any]]) -> pd.DataFrame:
    """Explode nested products into line items for analytics aggregation."""
    orders_df = pd.DataFrame(orders)
    exploded_df = orders_df.loc[:, ["order_id", "products"]].explode("products", ignore_index=True)

    if exploded_df["products"].isna().any():
        raise DummyJSONAnalyticsError("One or more orders do not contain product rows.")

    line_items_df = pd.json_normalize(exploded_df.pop("products")).rename(
        columns={
            "id": "product_id",
            "title": "product_title",
            "category": "category",
            "price": "price",
            "quantity": "quantity",
            "discount": "discount",
        }
    )
    lines_df = pd.concat([exploded_df, line_items_df], axis=1)

    required_columns = {"product_id", "category", "price", "quantity", "discount"}
    missing_columns = required_columns - set(lines_df.columns)
    if missing_columns:
        raise DummyJSONAnalyticsError(
            f"Order product rows are missing required fields: {sorted(missing_columns)}."
        )

    lines_df["order_id"] = pd.to_numeric(lines_df["order_id"], errors="raise").astype("int64")
    lines_df["product_id"] = pd.to_numeric(lines_df["product_id"], errors="raise").astype("int64")
    lines_df["price"] = pd.to_numeric(lines_df["price"], errors="raise").round(2)
    lines_df["quantity"] = pd.to_numeric(lines_df["quantity"], errors="raise").astype("int64")
    lines_df["discount"] = pd.to_numeric(lines_df["discount"], errors="raise").round(2)
    lines_df["gross_line_amount"] = (lines_df["price"] * lines_df["quantity"]).round(2)
    lines_df["discount_amount"] = (
        lines_df["gross_line_amount"] * (lines_df["discount"] / 100.0)
    ).round(2)
    lines_df["net_line_amount"] = (
        lines_df["gross_line_amount"] - lines_df["discount_amount"]
    ).round(2)

    return lines_df


def _build_order_analytics_frame(
    orders_df: pd.DataFrame,
    product_lines_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build the order_analytics table."""
    aggregates_df = (
        product_lines_df.groupby("order_id", as_index=False, sort=False)
        .agg(
            total_items=("quantity", "sum"),
            gross_amount=("gross_line_amount", "sum"),
            total_discount_amount=("discount_amount", "sum"),
            unique_products=("product_id", "nunique"),
        )
        .copy()
    )
    aggregates_df["gross_amount"] = aggregates_df["gross_amount"].round(2)
    aggregates_df["total_discount_amount"] = aggregates_df["total_discount_amount"].round(2)
    aggregates_df["net_amount"] = (
        aggregates_df["gross_amount"] - aggregates_df["total_discount_amount"]
    ).round(2)
    aggregates_df["discount_ratio"] = (
        aggregates_df["total_discount_amount"]
        .div(aggregates_df["gross_amount"].where(aggregates_df["gross_amount"] != 0))
        .fillna(0.0)
        .round(4)
    )
    aggregates_df["order_complexity_score"] = (
        aggregates_df["unique_products"] * 2 + aggregates_df["total_items"]
    ).astype("int64")

    dominant_category_df = (
        product_lines_df.groupby(["order_id", "category"], as_index=False, sort=False)
        .agg(category_contribution=("gross_line_amount", "sum"))
        .sort_values(
            ["order_id", "category_contribution", "category"],
            ascending=[True, False, True],
        )
        .drop_duplicates(subset=["order_id"])
        .loc[:, ["order_id", "category"]]
        .rename(columns={"category": "dominant_category"})
    )

    order_analytics_df = (
        orders_df.loc[:, ["order_id", "customer_id", "order_timestamp", "shipping_cost"]]
        .drop_duplicates(subset=["order_id"])
        .merge(aggregates_df, on="order_id", how="inner", validate="one_to_one")
        .merge(dominant_category_df, on="order_id", how="inner", validate="one_to_one")
    )
    order_analytics_df["order_date"] = order_analytics_df["order_timestamp"].dt.strftime("%Y-%m-%d")
    order_analytics_df["order_hour"] = order_analytics_df["order_timestamp"].dt.hour.astype("int64")
    order_analytics_df["final_amount"] = (
        order_analytics_df["net_amount"] + order_analytics_df["shipping_cost"]
    ).round(2)

    return order_analytics_df[
        [
            "order_id",
            "customer_id",
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
        ]
    ].sort_values("order_id")


def _build_customer_analytics_frame(
    orders_df: pd.DataFrame,
    order_analytics_df: pd.DataFrame,
    as_of_date: date,
) -> pd.DataFrame:
    """Build the customer_analytics table."""
    customer_base_df = (
        orders_df.loc[:, ["customer_id", "full_name", "email", "city", "signup_date"]]
        .drop_duplicates(subset=["customer_id"])
        .copy()
    )
    customer_base_df["email_domain"] = customer_base_df["email"].str.split("@").str[-1]
    customer_base_df["customer_tenure_days"] = (
        pd.Timestamp(as_of_date)
        - pd.to_datetime(customer_base_df["signup_date"])
    ).dt.days.clip(lower=0).astype("int64")

    customer_metrics_df = (
        order_analytics_df.groupby("customer_id", as_index=False, sort=False)
        .agg(
            total_orders=("order_id", "count"),
            total_spent=("final_amount", "sum"),
        )
        .copy()
    )
    customer_metrics_df["total_spent"] = customer_metrics_df["total_spent"].round(2)
    customer_metrics_df["avg_order_value"] = (
        customer_metrics_df["total_spent"] / customer_metrics_df["total_orders"]
    ).round(2)

    customer_analytics_df = customer_base_df.merge(
        customer_metrics_df,
        on="customer_id",
        how="inner",
        validate="one_to_one",
    )

    spent_score = _min_max_normalize(customer_analytics_df["total_spent"])
    order_score = _min_max_normalize(customer_analytics_df["total_orders"])
    aov_score = _min_max_normalize(customer_analytics_df["avg_order_value"])
    customer_analytics_df["lifetime_value_score"] = (
        (spent_score * 0.5 + order_score * 0.3 + aov_score * 0.2) * 100
    ).round(2)
    customer_analytics_df["customer_segment"] = customer_analytics_df["lifetime_value_score"].apply(
        _segment_customer
    )

    return customer_analytics_df[
        [
            "customer_id",
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
        ]
    ].sort_values("customer_id")


def _frame_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame to JSON-serializable records."""
    records: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        normalized_record: dict[str, Any] = {}
        for key, value in record.items():
            if pd.isna(value):
                normalized_record[key] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                normalized_record[key] = value.isoformat()
            elif isinstance(value, date):
                normalized_record[key] = value.isoformat()
            elif hasattr(value, "item"):
                normalized_record[key] = value.item()
            else:
                normalized_record[key] = value
        records.append(normalized_record)
    return records


def _normalize_as_of_date(value: date | str | None) -> date:
    """Normalize an as-of date input."""
    if value is None:
        return datetime.now().date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _min_max_normalize(series: pd.Series) -> pd.Series:
    """Normalize a numeric series into the 0..1 range."""
    numeric_series = pd.to_numeric(series, errors="raise").astype("float64")
    min_value = numeric_series.min()
    max_value = numeric_series.max()
    if pd.isna(min_value) or pd.isna(max_value):
        raise DummyJSONAnalyticsError("Cannot normalize an empty numeric series.")
    if max_value == min_value:
        return pd.Series([1.0] * len(numeric_series), index=numeric_series.index)
    return (numeric_series - min_value) / (max_value - min_value)


def _segment_customer(score: float) -> str:
    """Bucket a lifetime value score into a simple segment."""
    if score >= 70:
        return "High"
    if score >= 40:
        return "Medium"
    return "Low"
