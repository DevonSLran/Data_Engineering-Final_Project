#!/usr/bin/env python3
"""
Bronze loader for the Olist medallion pipeline.

Reads the raw Olist CSVs from data/raw and loads each one, untransformed,
into the ``bronze`` schema of the warehouse Postgres. Every column lands as
TEXT — bronze is a faithful copy of the source; type casting and cleaning
happen later in the dbt staging (silver) layer.

The load is IDEMPOTENT: each table is dropped and recreated on every run, so
re-running produces exactly the same state (a full refresh). Loading uses
COPY for speed (the geolocation file alone is ~1M rows).

Connection + paths are read from environment variables (see .env.example):
    WAREHOUSE_HOST, WAREHOUSE_PORT, WAREHOUSE_DB, WAREHOUSE_USER,
    WAREHOUSE_PASSWORD, BRONZE_SCHEMA, RAW_DATA_DIR
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

# --- the nine Olist source files and the bronze table each one lands in -----
# (we strip the cosmetic ``olist_`` prefix and ``_dataset`` suffix so the
#  warehouse tables read cleanly: olist_orders_dataset.csv -> bronze.orders)
FILE_TO_TABLE = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation",
}


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=os.environ.get("WAREHOUSE_PORT", "5432"),
        dbname=os.environ.get("WAREHOUSE_DB", "olist"),
        user=os.environ.get("WAREHOUSE_USER", "olist"),
        password=os.environ.get("WAREHOUSE_PASSWORD", "olist"),
    )


def read_header(csv_path: Path) -> list[str]:
    """Return the column names, stripping any UTF-8 BOM on the first cell."""
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        header = next(csv.reader(fh))
    return [col.strip() for col in header]


def load_table(cur, schema: str, table: str, csv_path: Path) -> int:
    columns = read_header(csv_path)

    cur.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
    )
    cur.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(
                sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns
            ),
        )
    )

    # COPY straight from the file. utf-8-sig handles the BOM; HEADER true skips
    # the header row. In CSV mode an unquoted empty field becomes NULL.
    copy_stmt = sql.SQL(
        "COPY {}.{} FROM STDIN WITH (FORMAT csv, HEADER true)"
    ).format(sql.Identifier(schema), sql.Identifier(table))
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        cur.copy_expert(copy_stmt.as_string(cur), fh)

    cur.execute(
        sql.SQL("SELECT count(*) FROM {}.{}").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
    )
    return cur.fetchone()[0]


def main() -> int:
    raw_dir = Path(os.environ.get("RAW_DATA_DIR", "data/raw"))
    schema = os.environ.get("BRONZE_SCHEMA", "bronze")

    if not raw_dir.is_dir():
        print(f"[bronze] raw data dir not found: {raw_dir}", file=sys.stderr)
        return 1

    missing = [f for f in FILE_TO_TABLE if not (raw_dir / f).is_file()]
    if missing:
        print(f"[bronze] missing source files in {raw_dir}: {missing}", file=sys.stderr)
        return 1

    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
            total = 0
            for filename, table in FILE_TO_TABLE.items():
                rows = load_table(cur, schema, table, raw_dir / filename)
                total += rows
                print(f"[bronze] {filename:45} -> {schema}.{table:35} {rows:>9,} rows")
        conn.commit()
        print(f"[bronze] done — {len(FILE_TO_TABLE)} tables, {total:,} rows total")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
