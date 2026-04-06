"""Configuration loader for the ETL project."""

from __future__ import annotations

import os
from pathlib import Path

import yaml


def load_config() -> dict[str, dict[str, str | int | bool]]:
    """Load configuration from config.yml and set environment variables."""
    config_path = Path(__file__).parent / "config.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Flatten and set environment variables
    for section, values in config.items():
        for key, value in values.items():
            env_key = f"{section.upper()}_{key.upper()}"
            if isinstance(value, bool):
                os.environ.setdefault(env_key, str(value).lower())
            elif isinstance(value, int):
                os.environ.setdefault(env_key, str(value))
            else:
                os.environ.setdefault(env_key, str(value))

    return config