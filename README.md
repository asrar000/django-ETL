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

At this stage, the repository contains a professional starter scaffold only. Business logic, Django settings implementation, ETL jobs, models, and tests will be added in later commits.

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
│   ├── loaders/
│   ├── orchestration/
│   ├── schemas/
│   ├── transformers/
│   └── utils/
├── logs/
├── requirements/
├── scripts/
└── tests/
    ├── fixtures/
    ├── integration/
    └── unit/
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
