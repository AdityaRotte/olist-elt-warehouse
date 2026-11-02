"""
Load the Olist CSVs into the warehouse's `raw` schema.

Every column lands as TEXT (a faithful copy of the source); typing and cleaning
happen later in dbt staging models. Each table also gets `_loaded_at`, which the
incremental dbt models use as their watermark.

Because the Olist dataset is a static historical export, we simulate daily
arrival: `day --date 2017-03-05` loads only the orders *purchased* on that date
(plus their items, payments and reviews). Re-running a date is safe (idempotent).

Usage:
    python ingestion/load_olist.py static            # dimensions, full reload
    python ingestion/load_olist.py day --date 2017-03-05
    python ingestion/load_olist.py all               # everything at once (fast dev start)
"""
import argparse
import io
import os
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2

DATA_DIR = Path(
    os.getenv("OLIST_DATA_DIR", Path(__file__).resolve().parents[1] / "data" / "raw" / "olist")
)

STATIC_FILES = {
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
ORDERS_FILE = "olist_orders_dataset.csv"
CHILD_FILES = {  # tables keyed to an order
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}


def connect():
    return psycopg2.connect(
        host=os.getenv("WH_HOST", "localhost"),
        port=os.getenv("WH_PORT", "5433"),
        user=os.getenv("WH_USER", "warehouse"),
        password=os.getenv("WH_PASSWORD", "warehouse"),
        dbname=os.getenv("WH_DB", "olist"),
    )


def read_csv(filename: str) -> pd.DataFrame:
    # dtype=str keeps raw data untouched; utf-8-sig strips a BOM if the file has one
    return pd.read_csv(DATA_DIR / filename, dtype=str, encoding="utf-8-sig")


def ensure_table(cur, table: str, columns: list[str]) -> None:
    cols = ", ".join(f'"{c}" text' for c in columns)
    cur.execute("create schema if not exists raw")
    cur.execute(f"create table if not exists raw.{table} ({cols}, _loaded_at timestamptz default now())")


def copy_rows(cur, table: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)  # NaN -> empty -> NULL on COPY
    buf.seek(0)
    col_list = ", ".join(f'"{c}"' for c in df.columns)
    cur.copy_expert(f"copy raw.{table} ({col_list}) from stdin with (format csv)", buf)


def reload_all(cur, table: str, df: pd.DataFrame) -> None:
    ensure_table(cur, table, list(df.columns))
    cur.execute(f"truncate table raw.{table}")
    copy_rows(cur, table, df)
    print(f"  raw.{table}: {len(df):,} rows (full reload)")


def reload_orders(cur, table: str, df: pd.DataFrame, order_ids: list[str]) -> None:
    """Delete-then-insert for a set of orders -> idempotent re-runs."""
    ensure_table(cur, table, list(df.columns))
    cur.execute(f"delete from raw.{table} where order_id = any(%s)", (order_ids,))
    copy_rows(cur, table, df)
    print(f"  raw.{table}: {len(df):,} rows")


def load_static() -> None:
    with connect() as conn, conn.cursor() as cur:
        for table, filename in STATIC_FILES.items():
            reload_all(cur, table, read_csv(filename))


def load_orders(day: date | None) -> None:
    """day=None loads every order; otherwise only orders purchased on `day`."""
    orders = read_csv(ORDERS_FILE)
    if day is not None:
        purchased = pd.to_datetime(orders["order_purchase_timestamp"]).dt.date
        orders = orders[purchased == day]
        if orders.empty:
            print(f"No orders purchased on {day}; nothing to load.")
            return
    ids = orders["order_id"].tolist()
    print(f"Loading {len(ids):,} orders" + (f" for {day}" if day else " (all)"))

    with connect() as conn, conn.cursor() as cur:
        # orders first, then children, all in one transaction
        reload_orders(cur, "orders", orders, ids)
        for table, filename in CHILD_FILES.items():
            child = read_csv(filename)
            reload_orders(cur, table, child[child["order_id"].isin(ids)], ids)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("static")
    sub.add_parser("all")
    d = sub.add_parser("day")
    d.add_argument("--date", required=True, help="YYYY-MM-DD (purchase date to load)")
    args = p.parse_args()

    if args.cmd == "static":
        load_static()
    elif args.cmd == "day":
        load_orders(date.fromisoformat(args.date))
    else:
        load_static()
        load_orders(None)


if __name__ == "__main__":
    main()
