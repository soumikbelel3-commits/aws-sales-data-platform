from datetime import date, timedelta

import pytest

from validation import line_revenue, validate_order


def good_order(**overrides):
    order = {
        "order_id": "ORD01001",
        "customer_id": "CUST0042",
        "order_date": "2026-10-01",
        "event_timestamp": "2026-10-01T10:15:00Z",
        "product": "Laptop",
        "city": "Kolkata",
        "quantity": 1,
        "unit_price": 65000,
        "payment_method": "UPI",
        "order_status": "DELIVERED",
    }
    order.update(overrides)
    return order


def test_valid_order_has_no_errors():
    assert validate_order(good_order()) == []


def test_numeric_strings_from_csv_are_accepted():
    assert validate_order(good_order(quantity="2", unit_price="2500.50")) == []


REQUIRED = ["order_id", "order_date", "product", "city", "quantity", "unit_price"]


@pytest.mark.parametrize("field", REQUIRED)
def test_missing_required_field(field):
    errors = validate_order(good_order(**{field: ""}))
    assert f"missing required field: {field}" in errors


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"order_id": "12345"}, "order_id must look like ORD followed by digits"),
        ({"order_date": "2026-13-45"}, "order_date must be YYYY-MM-DD"),
        ({"quantity": -1}, "quantity must be a positive whole number"),
        ({"quantity": 1.5}, "quantity must be a positive whole number"),
        ({"quantity": True}, "quantity must be a positive whole number"),
        ({"quantity": 500}, "quantity must be <= 100"),
        ({"unit_price": "abc"}, "unit_price must be a positive number"),
        ({"unit_price": 0}, "unit_price must be a positive number"),
        ({"event_timestamp": "yesterday"}, "event_timestamp must be an ISO-8601 timestamp"),
    ],
)
def test_invalid_values(overrides, expected):
    assert expected in validate_order(good_order(**overrides))


def test_future_order_date_rejected():
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    assert "order_date cannot be in the future" in validate_order(good_order(order_date=tomorrow))


def test_unknown_payment_method_rejected():
    errors = validate_order(good_order(payment_method="Bitcoin"))
    assert any(e.startswith("payment_method must be one of") for e in errors)


def test_non_dict_rejected():
    assert validate_order(["not", "a", "dict"]) == ["record must be a JSON object"]


def test_line_revenue():
    assert line_revenue(good_order(quantity="3", unit_price="2500")) == 7500.0
