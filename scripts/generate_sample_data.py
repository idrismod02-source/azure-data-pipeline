"""
Generate realistic sample sales data for local testing.
Produces CSV files that mirror the REST API output schema.

Usage:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --rows 100000 --years 2
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

REGIONS = ["NORTH_EAST", "SOUTH_EAST", "MIDWEST", "SOUTH_WEST", "WEST_COAST"]
CATEGORIES = ["ELECTRONICS", "CLOTHING", "HOME_GARDEN", "SPORTS", "BOOKS", "BEAUTY"]
STATUSES = ["COMPLETED"] * 75 + ["PENDING"] * 10 + ["CANCELLED"] * 8 + \
           ["REFUNDED"] * 5 + ["PROCESSING"] * 2

PRODUCTS = {
    "ELECTRONICS": [f"ELEC-{i:04d}" for i in range(1, 51)],
    "CLOTHING":    [f"CLTH-{i:04d}" for i in range(1, 41)],
    "HOME_GARDEN": [f"HOME-{i:04d}" for i in range(1, 31)],
    "SPORTS":      [f"SPRT-{i:04d}" for i in range(1, 26)],
    "BOOKS":       [f"BOOK-{i:04d}" for i in range(1, 21)],
    "BEAUTY":      [f"BEAU-{i:04d}" for i in range(1, 21)],
}

PRICE_RANGES = {
    "ELECTRONICS": (29.99, 1299.99),
    "CLOTHING":    (9.99,  299.99),
    "HOME_GARDEN": (14.99, 499.99),
    "SPORTS":      (19.99, 599.99),
    "BOOKS":       (4.99,  49.99),
    "BEAUTY":      (7.99,  149.99),
}

CUSTOMER_POOL = [f"CUST-{i:06d}" for i in range(1, 5001)]  # 5,000 customers


def generate_order(order_date: datetime) -> dict:
    category = random.choice(CATEGORIES)
    lo, hi = PRICE_RANGES[category]
    quantity = random.choices([1, 2, 3, 4, 5], weights=[55, 25, 12, 5, 3])[0]
    unit_price = round(random.uniform(lo, hi), 2)

    # 5 % chance of a duplicate (simulates real-world API retries)
    order_id = str(uuid.uuid4()) if random.random() > 0.05 else f"DUP-{uuid.uuid4()}"

    # last_modified slightly after order_date to simulate processing delay
    last_modified = order_date + timedelta(
        hours=random.randint(0, 48),
        minutes=random.randint(0, 59)
    )

    return {
        "order_id":         order_id,
        "customer_id":      random.choice(CUSTOMER_POOL),
        "product_id":       random.choice(PRODUCTS[category]),
        "product_category": category,
        "order_date":       order_date.strftime("%Y-%m-%d"),
        "order_amount":     round(unit_price * quantity, 2),
        "quantity":         quantity,
        "region":           random.choice(REGIONS),
        "status":           random.choice(STATUSES),
        "last_modified":    last_modified.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate sample sales data")
    parser.add_argument("--rows",  type=int, default=50_000,
                        help="Number of order rows to generate (default: 50000)")
    parser.add_argument("--years", type=int, default=2,
                        help="Years of history to span (default: 2)")
    args = parser.parse_args()

    out_dir = Path("data/sample")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "orders_sample.csv"

    end_date   = datetime.now()
    start_date = end_date - timedelta(days=365 * args.years)

    fieldnames = [
        "order_id", "customer_id", "product_id", "product_category",
        "order_date", "order_amount", "quantity", "region", "status",
        "last_modified"
    ]

    print(f"Generating {args.rows:,} orders spanning {args.years} year(s)...")

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        total_revenue = 0.0
        for i in range(args.rows):
            order_date = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            order = generate_order(order_date)
            writer.writerow(order)
            total_revenue += order["order_amount"]

            if (i + 1) % 10_000 == 0:
                print(f"  {i + 1:,} rows written...")

    file_size_mb = out_file.stat().st_size / (1024 * 1024)

    print(f"\n✅ Sample data written to: {out_file}")
    print(f"   Rows       : {args.rows:,}")
    print(f"   File size  : {file_size_mb:.1f} MB")
    print(f"   Gross rev  : ${total_revenue:,.2f}")
    print(f"   Date range : {start_date.date()} → {end_date.date()}")
    print(f"\nNext step:")
    print(f"  Upload to ADLS Gen2 raw zone or run locally with PySpark.")


if __name__ == "__main__":
    main()
