import csv
from collections import Counter
from datetime import date

from generate_data import FIELDS, generate, write_csv
from validation import validate_order

START = date(2026, 10, 1)


def test_clean_data_is_all_valid_and_unique():
    records = generate(rows=500, start=START, days=12, bad_rate=0, dup_rate=0, seed=1)
    assert len(records) == 500
    assert all(validate_order(r) == [] for r in records)
    assert len({r["order_id"] for r in records}) == 500


def test_bad_rows_and_duplicates_are_injected():
    records = generate(rows=1000, start=START, days=12, bad_rate=0.05, dup_rate=0.02, seed=1)
    invalid = [r for r in records if validate_order(r)]
    ids = Counter(r["order_id"] for r in records if r["order_id"])
    duplicated = [i for i, n in ids.items() if n > 1]

    assert len(records) > 1000
    assert invalid, "expected some invalid rows"
    assert duplicated, "expected some duplicate order_ids"


def test_same_seed_is_reproducible():
    a = generate(rows=100, start=START, days=5, bad_rate=0.1, dup_rate=0.1, seed=7)
    b = generate(rows=100, start=START, days=5, bad_rate=0.1, dup_rate=0.1, seed=7)
    assert a == b


def test_write_csv(tmp_path):
    records = generate(rows=10, start=START, days=2, bad_rate=0, dup_rate=0, seed=1)
    out = tmp_path / "nested" / "sales.csv"
    write_csv(records, out)

    with out.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 10
    assert list(rows[0].keys()) == FIELDS
