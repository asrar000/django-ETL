"""Extraction logic for DummyJSON carts, users, and products."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from etl.utils.paths import RAW_DATA_DIR, ensure_directory


LOGGER = logging.getLogger(__name__)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "django-etl/0.1",
}


@dataclass(frozen=True, slots=True)
class ResourceConfig:
    """Configuration for a DummyJSON collection endpoint."""

    name: str
    path: str
    collection_key: str
    required_keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExtractionArtifact:
    """Metadata describing a saved raw extraction snapshot."""

    resource_name: str
    endpoint: str
    record_count: int
    output_path: Path
    extracted_at: str


RESOURCE_CONFIGS: dict[str, ResourceConfig] = {
    "carts": ResourceConfig(
        name="carts",
        path="/carts",
        collection_key="carts",
        required_keys=("id", "userId", "products"),
    ),
    "users": ResourceConfig(
        name="users",
        path="/users",
        collection_key="users",
        required_keys=("id", "firstName", "lastName", "email"),
    ),
    "products": ResourceConfig(
        name="products",
        path="/products",
        collection_key="products",
        required_keys=("id", "title", "category", "price"),
    ),
}


class DummyJSONExtractionError(RuntimeError):
    """Raised when the DummyJSON extractor cannot complete a request."""


@dataclass(slots=True)
class DummyJSONExtractor:
    """Extract paginated collections from DummyJSON and save raw snapshots."""

    base_url: str = "https://dummyjson.com"
    raw_root: Path = RAW_DATA_DIR
    page_size: int = 100
    timeout_seconds: int = 30
    max_retries: int = 4
    backoff_factor: float = 1.0

    def extract_all(self) -> list[ExtractionArtifact]:
        """Extract the supported DummyJSON collections."""
        artifacts: list[ExtractionArtifact] = []

        for resource_name in RESOURCE_CONFIGS:
            artifacts.append(self.extract_resource(resource_name))

        return artifacts

    def extract_resource(self, resource_name: str) -> ExtractionArtifact:
        """Extract one configured resource and persist it as raw JSON."""
        if resource_name not in RESOURCE_CONFIGS:
            available = ", ".join(sorted(RESOURCE_CONFIGS))
            raise ValueError(f"Unsupported resource '{resource_name}'. Expected one of: {available}")

        config = RESOURCE_CONFIGS[resource_name]
        extracted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        records = self._fetch_paginated_collection(config)
        self._validate_records(config, records)
        output_path = self._write_snapshot(config, records, extracted_at)

        LOGGER.info(
            "Saved %s raw snapshot with %s records to %s",
            config.name,
            len(records),
            output_path,
        )

        return ExtractionArtifact(
            resource_name=config.name,
            endpoint=f"{self.base_url.rstrip('/')}{config.path}",
            record_count=len(records),
            output_path=output_path,
            extracted_at=extracted_at,
        )

    def _fetch_paginated_collection(self, config: ResourceConfig) -> list[dict[str, Any]]:
        """Fetch all pages for a DummyJSON collection."""
        records: list[dict[str, Any]] = []
        skip = 0
        total_records: int | None = None

        while True:
            params = {"limit": self.page_size, "skip": skip}
            response = self._request_json(config.path, params=params)
            page_records = response.get(config.collection_key, [])

            if not isinstance(page_records, list):
                raise DummyJSONExtractionError(
                    f"Expected '{config.collection_key}' to be a list for resource '{config.name}'."
                )

            if total_records is None:
                total_records = int(response.get("total", len(page_records)))
                LOGGER.info(
                    "Started extracting %s. API reported %s total records.",
                    config.name,
                    total_records,
                )

            if not page_records:
                break

            records.extend(page_records)
            skip += len(page_records)

            LOGGER.info(
                "Fetched %s %s records so far.",
                len(records),
                config.name,
            )

            if total_records is not None and skip >= total_records:
                break

        return records

    def _request_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Request JSON from DummyJSON with retries and exponential backoff."""
        request_url = self._build_url(path, params or {})

        for attempt in range(1, self.max_retries + 1):
            request = Request(request_url, headers=DEFAULT_HEADERS)

            try:
                LOGGER.info("Requesting %s (attempt %s/%s)", request_url, attempt, self.max_retries)
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as error:
                status_code = error.code
                if status_code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    self._sleep_before_retry(path, attempt, status_code)
                    continue

                raise DummyJSONExtractionError(
                    f"Request to {request_url} failed with status {status_code}."
                ) from error
            except URLError as error:
                if attempt < self.max_retries:
                    self._sleep_before_retry(path, attempt, str(error.reason))
                    continue

                raise DummyJSONExtractionError(
                    f"Request to {request_url} failed due to a network error: {error.reason}."
                ) from error

        raise DummyJSONExtractionError(f"Failed to retrieve data from {request_url}.")

    def _sleep_before_retry(self, path: str, attempt: int, reason: int | str) -> None:
        """Sleep before retrying a transient failure."""
        delay_seconds = self.backoff_factor * (2 ** (attempt - 1))
        LOGGER.warning(
            "Transient extraction failure for %s because of %s. Retrying in %.1f seconds.",
            path,
            reason,
            delay_seconds,
        )
        time.sleep(delay_seconds)

    def _write_snapshot(
        self,
        config: ResourceConfig,
        records: list[dict[str, Any]],
        extracted_at: str,
    ) -> Path:
        """Persist a raw extraction snapshot to disk."""
        resource_directory = ensure_directory(self.raw_root / config.name)
        safe_timestamp = extracted_at.replace("-", "").replace(":", "")
        output_path = resource_directory / f"dummyjson_{config.name}_{safe_timestamp}.json"

        payload = {
            "resource": config.name,
            "source": "DummyJSON",
            "endpoint": f"{self.base_url.rstrip('/')}{config.path}",
            "extracted_at": extracted_at,
            "record_count": len(records),
            "records": records,
        }

        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return output_path

    def _build_url(self, path: str, params: dict[str, Any]) -> str:
        """Build a request URL from a path and query string parameters."""
        base = f"{self.base_url.rstrip('/')}{path}"
        query = urlencode(params)
        return f"{base}?{query}" if query else base

    def _validate_records(self, config: ResourceConfig, records: list[dict[str, Any]]) -> None:
        """Validate that extracted records contain the expected minimum keys."""
        if not records:
            raise DummyJSONExtractionError(f"No records were returned for resource '{config.name}'.")

        duplicate_ids: set[Any] = set()
        seen_ids: set[Any] = set()

        for index, record in enumerate(records):
            missing_keys = [key for key in config.required_keys if key not in record]
            if missing_keys:
                raise DummyJSONExtractionError(
                    f"Resource '{config.name}' record at index {index} is missing keys: {missing_keys}."
                )

            record_id = record["id"]
            if record_id in seen_ids:
                duplicate_ids.add(record_id)
            seen_ids.add(record_id)

        if duplicate_ids:
            raise DummyJSONExtractionError(
                f"Resource '{config.name}' contains duplicate record ids: {sorted(duplicate_ids)}."
            )
