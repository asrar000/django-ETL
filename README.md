# Django ETL Pipeline

This is a **Django-based ETL project** that extracts e-commerce data from the DummyJSON API, transforms it using Pandas, and loads analytical outputs into PostgreSQL through the Django ORM.

The project combines Django's ORM and management commands with a complete extract-transform-load (ETL) pipeline for scalable data processing.

**Key features:**

- resilient API extraction with retries and exponential backoff
- pandas-based transformation of nested JSON payloads
- loading curated analytics tables through Django ORM models (with upsert logic to avoid duplicates)
- structured logging, testability, and maintainable project separation
- PostgreSQL persistence via Docker for isolated, reproducible data storage

The initial analytics targets for this project are:

- `customer_analytics` – customer dimension with metrics and segmentation
- `order_analytics` – order dimension with business logic and derived fields

This project requires **one terminal** to run all components sequentially (Docker PostgreSQL runs detached in the background).

## Current Status

The repository now includes the initial extraction layer for DummyJSON:

- paginated extraction for `carts`, `users`, and `products`
- retries with exponential backoff for transient API failures
- raw JSON snapshot persistence under `data/raw/`
- a small runner script for local extraction
- pandas-based deterministic synthetic enrichment for `order_timestamp`, `signup_date`, `payment_details`, and `shipping_cost`
- a pandas-based synthetic order generator that scales the working dataset to 10,000 rows
- a pandas-based analytics transformation stage that builds `customer_analytics` and `order_analytics`
- **Django ORM models and management commands** for PostgreSQL persistence with upsert logic

## Quick Start

To run the complete project from environment setup to data loaded in PostgreSQL:

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install --upgrade pip
pip install -r requirements/base.txt

# 3. Start PostgreSQL in Docker (runs in background)
docker compose up -d postgres

# 4. Setup Django and run migrations
export DJANGO_SETTINGS_MODULE=config.settings.base
python manage.py migrate

# 5. Run the ETL pipeline (all steps sequentially)
python scripts/extract_dummyjson.py
python scripts/enrich_dummyjson_orders.py
python scripts/generate_dummyjson_synthetic_orders.py --target-rows 10000
python scripts/transform_dummyjson_analytics.py --source auto --as-of-date 2026-04-03

# 6. Load analytics data into PostgreSQL via Django ORM
python manage.py load_dummyjson_analytics --source auto --as-of-date 2026-04-03

# 7. Verify data loaded
docker compose exec postgres psql -U django_etl -d django_etl -c "SELECT COUNT(*) FROM customer_analytics; SELECT COUNT(*) FROM order_analytics;"
```

**That's it!** All steps run in a single terminal. Docker PostgreSQL runs detached (background), freeing the terminal for ETL commands.

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

This project requires:

- `python3`
- `python3-venv`
- `pip`
- `docker` (to run PostgreSQL without system-level DB setup)

You can verify them with:

```bash
python3 --version
python3 -m pip --version
docker ps  # If permission denied, run: sudo usermod -aG docker $USER (then re-login)
```

### Step 1: Create the virtual environment

From the project root, run:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 2: Install the project dependencies

```bash
pip install --upgrade pip
pip install -r requirements/base.txt
```

### Step 3: Start PostgreSQL in Docker

```bash
docker compose up -d postgres
```

Verify the container is running and healthy:

```bash
docker compose ps
# Wait until STATUS shows "healthy" (healthcheck passes)
```

### Step 4: Setup Django database

Set the Django settings module and apply migrations:

```bash
export DJANGO_SETTINGS_MODULE=config.settings.base
python manage.py migrate
```

Verify the connection:

```bash
python manage.py dbshell
# Type: \q  (to exit psql)
```

### Step 5: Run the ETL pipeline

Extract, transform, and load data (see **Current Workflow** section below for details).

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
# Ensure Django environment is set
export DJANGO_SETTINGS_MODULE=config.settings.base

# Extract fresh data from DummyJSON API
python scripts/extract_dummyjson.py

# Enrich orders with synthetic fields (pandas)
python scripts/enrich_dummyjson_orders.py

# Generate 10,000-row synthetic dataset
python scripts/generate_dummyjson_synthetic_orders.py --target-rows 10000

# Transform data into analytics tables (pandas)
python scripts/transform_dummyjson_analytics.py --source auto --as-of-date 2026-04-03

# Load analytics data into PostgreSQL using Django ORM
python manage.py load_dummyjson_analytics --source auto --as-of-date 2026-04-03
```

This means:

- the raw DummyJSON source is fetched fresh on every extraction run
- the raw API snapshots remain unchanged in `data/raw/`
- the pandas transform creates the enriched working dataset separately in `data/interim/orders/`
- the synthetic generation stage creates the larger 10,000-row dataset separately in `data/interim/orders/`
- the analytics transformation stage writes the processed `customer_analytics` and `order_analytics` tables to `data/processed/`
- **the Django load step upserts (insert or update, never duplicate) the analytics data into PostgreSQL**

## Loading Data into PostgreSQL

The final step of the ETL pipeline is loading transformed analytics data into PostgreSQL using Django ORM.

### Load Command

```bash
python manage.py load_dummyjson_analytics --source auto --as-of-date 2026-04-03
```

**Options:**
- `--source {auto|synthetic|enriched}` – Which interim order dataset to use (default: `auto` prefers synthetic)
- `--as-of-date YYYY-MM-DD` – Reference date for `customer_tenure_days` calculation
- `--batch-size N` – ORM bulk upsert batch size (default: 1000)
- `--log-level {DEBUG|INFO|WARNING|ERROR}` – Logging level

### Data Operation (Upsert Behavior)

The load process uses Django ORM's `bulk_create` with `update_conflicts=True`, which performs an **upsert**:

- **If a record exists** (matched by primary key: `customer_id` or `order_id`), it **updates** all fields with new values
- **If a record doesn't exist**, it **inserts** a new row
- **No duplicates** are created across multiple runs
- All operations execute within a database transaction (`transaction.atomic()`)

**Example:** Running the load twice:
1. **First run**: Inserts 100 customers + 500 orders
2. **Second run**: Updates same 100 customers + 500 orders (no new rows inserted)

To perform a full fresh reload:

```bash
python manage.py dbshell
# Inside psql:
TRUNCATE customer_analytics, order_analytics;
\q

# Then run load again:
python manage.py load_dummyjson_analytics --source auto --as-of-date 2026-04-03
```

## Viewing Data in PostgreSQL

### Connect to PostgreSQL container

```bash
docker compose exec postgres psql -U django_etl -d django_etl
```

### Common Queries

Inside the psql prompt:

```sql
-- Table counts
SELECT COUNT(*) FROM customer_analytics;
SELECT COUNT(*) FROM order_analytics;

-- Top customers by lifetime value
SELECT customer_id, full_name, email_domain, customer_segment, lifetime_value_score 
FROM customer_analytics 
ORDER BY lifetime_value_score DESC 
LIMIT 10;

-- Sample orders
SELECT order_id, customer_id, order_date, final_amount, dominant_category 
FROM order_analytics 
ORDER BY order_id 
LIMIT 10;

-- Table structure
\d customer_analytics;
\d order_analytics;

-- Exit
\q
```

### One-liner Query (without entering psql)

```bash
docker compose exec postgres psql -U django_etl -d django_etl -c "SELECT COUNT(*) FROM customer_analytics;"
```

## Running Tests

```bash
# Run all unit tests
python -m unittest tests.unit.test_dummyjson_extractor tests.unit.test_dummyjson_enrichment tests.unit.test_dummyjson_synthetic_orders tests.unit.test_dummyjson_analytics -v

# Run specific test module
python -m unittest tests.unit.test_dummyjson_analytics -v
```
