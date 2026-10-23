"""Validation rules for a single sales order record.

These rules are shared by the streaming ingestion Lambda and the tests.
The Glue batch job applies the same rules in PySpark (see glue_jobs/transform_sales.py).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

REQUIRED_FIELDS = (
    "order_id",
    "order_date",
    "product",
    "city",
    "quantity",
    "unit_price",
)

PAYMENT_METHODS = {"UPI", "Card", "NetBanking", "COD"}
ORDER_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"}

ORDER_ID_PATTERN = re.compile(r"^ORD\d{4,}$")
MAX_QUANTITY = 100


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _parse_positive_number(value: Any, cast: type) -> float | int | None:
    """Return value as int/float if it is a positive number, else None."""
    if isinstance(value, bool):
        return None
    try:
        number = cast(value)
    except (TypeError, ValueError):
        return None
    if cast is int and isinstance(value, float) and not value.is_integer():
        return None
    return number if number > 0 else None


def validate_order(record: dict[str, Any]) -> list[str]:
    """Return a list of validation errors. An empty list means the record is valid."""
    if not isinstance(record, dict):
        return ["record must be a JSON object"]

    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if _is_blank(record.get(field)):
            errors.append(f"missing required field: {field}")

    order_id = record.get("order_id")
    if not _is_blank(order_id) and not ORDER_ID_PATTERN.match(str(order_id)):
        errors.append("order_id must look like ORD followed by digits")

    order_date = record.get("order_date")
    if not _is_blank(order_date):
        try:
            parsed = date.fromisoformat(str(order_date))
            if parsed > date.today():
                errors.append("order_date cannot be in the future")
        except ValueError:
            errors.append("order_date must be YYYY-MM-DD")

    quantity = record.get("quantity")
    if not _is_blank(quantity):
        qty = _parse_positive_number(quantity, int)
        if qty is None:
            errors.append("quantity must be a positive whole number")
        elif qty > MAX_QUANTITY:
            errors.append(f"quantity must be <= {MAX_QUANTITY}")

    unit_price = record.get("unit_price")
    if not _is_blank(unit_price) and _parse_positive_number(unit_price, float) is None:
        errors.append("unit_price must be a positive number")

    payment_method = record.get("payment_method")
    if not _is_blank(payment_method) and payment_method not in PAYMENT_METHODS:
        errors.append(f"payment_method must be one of {sorted(PAYMENT_METHODS)}")

    order_status = record.get("order_status")
    if not _is_blank(order_status) and order_status not in ORDER_STATUSES:
        errors.append(f"order_status must be one of {sorted(ORDER_STATUSES)}")

    event_timestamp = record.get("event_timestamp")
    if not _is_blank(event_timestamp):
        try:
            datetime.fromisoformat(str(event_timestamp).replace("Z", "+00:00"))
        except ValueError:
            errors.append("event_timestamp must be an ISO-8601 timestamp")

    return errors


def line_revenue(record: dict[str, Any]) -> float:
    """Revenue for one order line: quantity x unit_price."""
    return round(int(record["quantity"]) * float(record["unit_price"]), 2)
