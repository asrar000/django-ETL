"""App configuration for analytics models."""

from __future__ import annotations

from django.apps import AppConfig


class AnalyticsConfig(AppConfig):
    """Configuration for the analytics app."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.analytics"
    verbose_name = "Analytics"
