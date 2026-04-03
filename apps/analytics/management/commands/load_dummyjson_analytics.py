"""Transform analytics data and load it into PostgreSQL with Django ORM."""

from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.analytics.models import CustomerAnalytics, OrderAnalytics
from etl.loaders import load_dummyjson_analytics
from etl.utils import configure_logging


class Command(BaseCommand):
    """Load transformed analytics tables into PostgreSQL."""

    help = "Build analytics datasets and load them into PostgreSQL using Django ORM."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--source",
            choices=("auto", "synthetic", "enriched"),
            default="auto",
            help="Which interim order dataset to transform before loading.",
        )
        parser.add_argument(
            "--as-of-date",
            help="As-of date for customer_tenure_days in YYYY-MM-DD format.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Batch size for ORM bulk upserts.",
        )
        parser.add_argument(
            "--log-level",
            default="INFO",
            help="Logging level for the load command.",
        )

    def handle(self, *args, **options):
        configure_logging(log_level=options["log_level"])

        artifact = load_dummyjson_analytics(
            source=options["source"],
            as_of_date=options["as_of_date"],
            batch_size=options["batch_size"],
        )

        self.stdout.write(
            self.style.SUCCESS(
                "Loaded analytics tables: "
                f"customer_rows={artifact.customer_rows_loaded}, "
                f"order_rows={artifact.order_rows_loaded}"
            )
        )
        self.stdout.write(
            f"customer_analytics file: {artifact.customer_output_path}"
        )
        self.stdout.write(
            f"order_analytics file: {artifact.order_output_path}"
        )

        top_customer = (
            CustomerAnalytics.objects.order_by("-lifetime_value_score")
            .values("customer_id", "full_name", "customer_segment")
            .first()
        )
        sample_order = (
            OrderAnalytics.objects.select_related("customer")
            .order_by("order_id")
            .values("order_id", "customer_id", "dominant_category")
            .first()
        )

        self.stdout.write(
            f"ORM counts: customers={CustomerAnalytics.objects.count()}, "
            f"orders={OrderAnalytics.objects.count()}"
        )
        if top_customer is not None:
            self.stdout.write(f"Top customer via ORM: {top_customer}")
        if sample_order is not None:
            self.stdout.write(f"Sample order via ORM: {sample_order}")
