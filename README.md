# Django ETL Pipeline

This repository contains the starter structure for an advanced Django ETL project focused on extracting e-commerce data from a dummy API, transforming it with Pandas, and loading analytical outputs into PostgreSQL through the Django ORM.

The implementation will be added incrementally, but the repository is being organized from the start to support:

- resilient API extraction with retries and exponential backoff
- pandas-based transformation of nested JSON payloads
- loading curated analytics tables through Django ORM models
- structured logging, testability, and maintainable project separation

The initial analytics targets for this project are:

- `customer_analytics`
- `order_analytics`

This `README.md` will be updated periodically as the project grows.

## Current Status

The repository now includes the initial extraction layer for DummyJSON:

- paginated extraction for `carts`, `users`, and `products`
- retries with exponential backoff for transient API failures
- raw JSON snapshot persistence under `data/raw/`
- a small runner script for local extraction
- pandas-based deterministic synthetic enrichment for `order_timestamp`, `signup_date`, `payment_details`, and `shipping_cost`
- a pandas-based synthetic order generator that scales the working dataset to 10,000 rows
- a pandas-based analytics transformation stage that builds `customer_analytics` and `order_analytics`

## Project Structure

```text
.
├── README.md
├── apps/
│   └── analytics/
│       └── migrations/
├── config/
│   └── settings/
├── data/
│   ├── interim/
│   ├── processed/
│   ├── raw/
│   └── sample/
├── docs/
│   └── architecture/
├── etl/
│   ├── extractors/
│   │   └── dummyjson.py
│   ├── loaders/
│   ├── orchestration/
│   │   ├── enrich_dummyjson_orders.py
│   │   ├── extract_dummyjson.py
│   │   ├── generate_dummyjson_synthetic_orders.py
│   │   └── transform_dummyjson_analytics.py
│   ├── schemas/
│   ├── transformers/
│   │   ├── dummyjson_analytics.py
│   │   ├── dummyjson_enrichment.py
│   │   └── dummyjson_synthetic_orders.py
│   └── utils/
├── logs/
├── requirements/
│   └── base.txt
├── scripts/
│   ├── enrich_dummyjson_orders.py
│   ├── extract_dummyjson.py
│   ├── generate_dummyjson_synthetic_orders.py
│   └── transform_dummyjson_analytics.py
└── tests/
    ├── fixtures/
    ├── integration/
    └── unit/
        ├── test_dummyjson_analytics.py
        ├── test_dummyjson_enrichment.py
        ├── test_dummyjson_extractor.py
        └── test_dummyjson_synthetic_orders.py
```

## Folder Overview

- `apps/analytics/` will contain Django models and app-level logic for analytics tables.
- `config/settings/` is reserved for environment-based Django settings organization.
- `etl/` will hold extraction, transformation, loading, orchestration, schema, and shared helper modules.
- `data/` separates raw, interim, processed, and sample datasets for local development.
- `tests/` is split into unit, integration, and fixture directories for maintainable test coverage.
- `docs/architecture/` will store design notes, ETL decisions, and operational documentation.
- `scripts/` is intended for helper commands such as local bootstrap and ETL runners.
- `requirements/` will store dependency files as the project setup is finalized.
- `logs/` is reserved for local ETL execution logs.

## Installation Guide

This project currently uses a local Python virtual environment and the dependency list in `requirements/base.txt`.

### Prerequisites

Make sure these are available on your machine:

- `python3`
- `python3-venv`
- `pip`

You can verify them with:

```bash
python3 --version
python3 -m pip --version
```

### 1. Create the virtual environment

From the project root, run:

```bash
python3 -m venv .venv
```

### 2. Install the project dependencies

```bash
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements/base.txt
```

### 3. Verify the pandas installation

```bash
.venv/bin/python - <<'PY'
import pandas as pd
print(pd.__version__)
PY
```

### 4. Run the extractor

This fetches fresh raw data every time you run it and stores timestamped snapshots in `data/raw/`.

```bash
.venv/bin/python scripts/extract_dummyjson.py
```

### 5. Run the pandas transformation step

This reads the latest raw snapshots, applies the deterministic synthetic enrichment logic with pandas, and writes the output to `data/interim/orders/`.

```bash
.venv/bin/python scripts/enrich_dummyjson_orders.py
```

### 6. Generate the 10,000-row synthetic order dataset

This uses the latest raw snapshots as the source of truth, builds the enriched working orders, and then scales them into a deterministic synthetic dataset for larger-volume analytics work.

```bash
.venv/bin/python scripts/generate_dummyjson_synthetic_orders.py --target-rows 10000
```

### 7. Run the current unit tests

```bash
.venv/bin/python -m unittest tests.unit.test_dummyjson_extractor tests.unit.test_dummyjson_enrichment tests.unit.test_dummyjson_synthetic_orders tests.unit.test_dummyjson_analytics
```

### 8. Build the analytics tables

This reads the latest interim order dataset, computes the required `customer_analytics` and `order_analytics` tables with pandas, and writes them to `data/processed/`.

```bash
.venv/bin/python scripts/transform_dummyjson_analytics.py --source auto --as-of-date 2026-04-03
```

## Extraction Usage

The extractor pulls:

- `https://dummyjson.com/carts`
- `https://dummyjson.com/users`
- `https://dummyjson.com/products`

Each resource is saved as a timestamped raw JSON snapshot inside its matching `data/raw/<resource>/` directory.

## Enrichment Usage

This transform step keeps the raw API response unchanged and adds synthetic fields in `data/interim/orders/`:

- `order_timestamp`
- `customer.signup_date`
- `payment_details`
- `shipping_cost`
- all transformations are executed with pandas DataFrames before the final JSON snapshot is written

## Synthetic 10000-Row Generation

The synthetic generation step:

- starts from the latest raw DummyJSON snapshots
- builds the enriched order dataset with pandas
- scales the order rows to a target count such as `10000`
- keeps the raw dataset unchanged
- writes the generated dataset separately inside `data/interim/orders/`

Run it with:

```bash
.venv/bin/python scripts/generate_dummyjson_synthetic_orders.py --target-rows 10000
```

## Analytics Transformation

The analytics transformation step builds two processed tables:

- `customer_analytics`
- `order_analytics`

Implemented customer logic:

- `full_name` from the customer profile in the order dataset
- lowercase email standardization and `email_domain` extraction
- `customer_tenure_days` from the selected as-of date minus `signup_date`
- `total_orders`, `total_spent`, and `avg_order_value` from order-level metrics
- `lifetime_value_score` from weighted normalized spend, order count, and average order value
- `customer_segment` as `High`, `Medium`, or `Low`

Implemented order logic:

- `order_date` and `order_hour` extracted from `order_timestamp`
- `gross_amount`, `total_discount_amount`, `net_amount`, and `final_amount`
- `discount_ratio` as `total_discount_amount / gross_amount`
- `order_complexity_score` as `(unique_products * 2) + total_items`
- `dominant_category` from the highest gross category contribution in the order

Run it with:

```bash
.venv/bin/python scripts/transform_dummyjson_analytics.py --source auto --as-of-date 2026-04-03
```

## Current Workflow

For the current stage of the project, the normal command order is:

```bash
.venv/bin/python scripts/extract_dummyjson.py
.venv/bin/python scripts/enrich_dummyjson_orders.py
.venv/bin/python scripts/generate_dummyjson_synthetic_orders.py --target-rows 10000
.venv/bin/python scripts/transform_dummyjson_analytics.py --source auto --as-of-date 2026-04-03
```

This means:

- the raw DummyJSON source is fetched fresh on every extraction run
- the raw API snapshots remain unchanged in `data/raw/`
- the pandas transform creates the enriched working dataset separately in `data/interim/orders/`
- the synthetic generation stage creates the larger 10,000-row dataset separately in `data/interim/orders/`
- the analytics transformation stage writes the processed `customer_analytics` and `order_analytics` tables to `data/processed/`
