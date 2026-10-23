"""Generate synthetic online-retail sales data as CSV.

By default it also injects a small share of invalid rows and duplicates,
so the ETL job's data-quality logic has something to catch.

Usage:
    python src/generate_data.py
    python src/generate_data.py --rows 5000 --bad-rate 0.03 --dup-rate 0.02
    python src/generate_data.py --clean          # no bad rows or duplicates
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

PRODUCTS = [
    ("Laptop", 65000),
    ("Headphones", 2500),
    ("Keyboard", 1800),
    ("Monitor", 12000),
    ("Mouse", 800),
    ("Webcam", 3200),
    ("SSD 1TB", 7500),
    ("Smartwatch", 9000),
]
CITIES = ["Kolkata", "Delhi", "Mumbai", "Bengaluru", "Chennai", "Hyderabad", "Pune"]
PAYMENT_METHODS = ["UPI", "Card", "NetBanking", "COD"]
ORDER_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
STATUS_WEIGHTS = [15, 20, 55, 6, 4]

FIELDS = [
    "order_id",
    "customer_id",
    "order_date",
    "event_timestamp",
    "product",
    "city",
    "quantity",
    "unit_price",
    "payment_method",
    "order_status",
]


def make_order(i: int, start: date, days: int, rng: random.Random) -> dict:
    product, price = rng.choice(PRODUCTS)
    order_day = start + timedelta(days=rng.randint(0, days - 1))
    event_time = datetime.combine(
        order_day,
        time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59)),
        tzinfo=UTC,
    )
    return {
        "order_id": f"ORD{i:05d}",
        "customer_id": f"CUST{rng.randint(1, 400):04d}",
        "order_date": order_day.isoformat(),
        "event_timestamp": event_time.isoformat().replace("+00:00", "Z"),
        "product": product,
        "city": rng.choice(CITIES),
        "quantity": rng.randint(1, 5),
        "unit_price": price,
        "payment_method": rng.choice(PAYMENT_METHODS),
        "order_status": rng.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0],
    }


def corrupt(order: dict, rng: random.Random) -> dict:
    """Break one field so the record fails validation."""
    bad = dict(order)
    problem = rng.choice(
        ["missing_id", "negative_qty", "bad_date", "bad_price", "missing_city"]
    )
    if problem == "missing_id":
        bad["order_id"] = ""
    elif problem == "negative_qty":
        bad["quantity"] = -rng.randint(1, 3)
    elif problem == "bad_date":
        bad["order_date"] = "2026-13-45"
    elif problem == "bad_price":
        bad["unit_price"] = "abc"
    else:
        bad["city"] = ""
    return bad


def generate(
    rows: int,
    start: date,
    days: int,
    bad_rate: float,
    dup_rate: float,
    seed: int,
) -> list[dict]:
    rng = random.Random(seed)
    records = []
    for i in range(1, rows + 1):
        order = make_order(i, start, days, rng)
        if rng.random() < bad_rate:
            order = corrupt(order, rng)
        records.append(order)

    # Duplicates: re-send an existing valid order (same order_id), as a retrying client would.
    n_dups = int(rows * dup_rate)
    for original in rng.sample(records, k=min(n_dups, len(records))):
        if original["order_id"]:
            records.append(dict(original))

    rng.shuffle(records)
    return records


def write_csv(records: list[dict], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--days", type=int, default=12)
    parser.add_argument("--bad-rate", type=float, default=0.02)
    parser.add_argument("--dup-rate", type=float, default=0.01)
    parser.add_argument("--clean", action="store_true", help="no bad rows or duplicates")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/sample_sales.csv"))
    args = parser.parse_args()

    bad_rate = 0.0 if args.clean else args.bad_rate
    dup_rate = 0.0 if args.clean else args.dup_rate

    records = generate(args.rows, args.start_date, args.days, bad_rate, dup_rate, args.seed)
    write_csv(records, args.output)
    print(f"Created {args.output} with {len(records)} records.")


if __name__ == "__main__":
    main()
