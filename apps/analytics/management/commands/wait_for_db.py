"""Wait for PostgreSQL to become available."""

from __future__ import annotations

import time

from django.core.management.base import BaseCommand
from django.db import connections
from django.db.utils import OperationalError


class Command(BaseCommand):
    """Poll the database until a connection succeeds."""

    help = "Wait until PostgreSQL is available."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--max-attempts", type=int, default=10)
        parser.add_argument("--initial-delay", type=float, default=1.0)

    def handle(self, *args, **options):
        max_attempts = options["max_attempts"]
        delay = options["initial_delay"]
        connection = connections["default"]

        for attempt in range(1, max_attempts + 1):
            try:
                connection.ensure_connection()
            except OperationalError as error:
                self.stdout.write(
                    self.style.WARNING(
                        f"Database unavailable (attempt {attempt}/{max_attempts}): {error}"
                    )
                )
                if attempt == max_attempts:
                    raise
                time.sleep(delay)
                delay *= 2
            else:
                self.stdout.write(self.style.SUCCESS("Database connection established."))
                return
