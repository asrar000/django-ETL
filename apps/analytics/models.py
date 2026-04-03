"""Django ORM models for analytics tables."""

from __future__ import annotations

from django.db import models


class CustomerAnalytics(models.Model):
    """Curated customer analytics facts."""

    customer_id = models.BigIntegerField(primary_key=True)
    full_name = models.CharField(max_length=255)
    email = models.EmailField(max_length=254)
    email_domain = models.CharField(max_length=255, db_index=True)
    city = models.CharField(max_length=255, blank=True, null=True)
    customer_tenure_days = models.PositiveIntegerField()
    total_orders = models.PositiveIntegerField()
    total_spent = models.DecimalField(max_digits=18, decimal_places=2)
    avg_order_value = models.DecimalField(max_digits=18, decimal_places=2)
    lifetime_value_score = models.DecimalField(max_digits=7, decimal_places=2)
    customer_segment = models.CharField(max_length=10, db_index=True)

    class Meta:
        db_table = "customer_analytics"
        ordering = ["customer_id"]

    def __str__(self) -> str:
        return f"{self.customer_id} - {self.full_name}"


class OrderAnalytics(models.Model):
    """Curated order analytics facts."""

    order_id = models.BigIntegerField(primary_key=True)
    customer = models.ForeignKey(
        CustomerAnalytics,
        on_delete=models.CASCADE,
        related_name="orders",
        db_column="customer_id",
        to_field="customer_id",
    )
    order_date = models.DateField()
    order_hour = models.PositiveSmallIntegerField()
    total_items = models.PositiveIntegerField()
    gross_amount = models.DecimalField(max_digits=18, decimal_places=2)
    total_discount_amount = models.DecimalField(max_digits=18, decimal_places=2)
    net_amount = models.DecimalField(max_digits=18, decimal_places=2)
    final_amount = models.DecimalField(max_digits=18, decimal_places=2)
    discount_ratio = models.DecimalField(max_digits=8, decimal_places=4)
    order_complexity_score = models.PositiveIntegerField()
    dominant_category = models.CharField(max_length=255, db_index=True)

    class Meta:
        db_table = "order_analytics"
        ordering = ["order_id"]

    def __str__(self) -> str:
        return str(self.order_id)
