#!/usr/bin/env python3
"""
Fetch the raw Olist CSVs into data/raw/.

The dataset's canonical home is Kaggle, but Kaggle downloads need an API token.
To keep `git clone` -> run friction-free, this script pulls the same nine files
from a public Hugging Face mirror and verifies each one against the canonical
Olist row counts. It is idempotent: a file that already exists with the right
row count is skipped, so re-running is cheap.

Usage:
    python ingestion/download_data.py            # download into ./data/raw
    RAW_DATA_DIR=/some/dir python ingestion/download_data.py
"""
from __future__ import annotations

import csv
import os
import sys
import urllib.request
from pathlib import Path

# Public mirror of the canonical Kaggle dataset (olistbr/brazilian-ecommerce).
BASE_URL = (
    "https://huggingface.co/datasets/aviahYadler/Olist_Ecommerce_Dataset/"
    "resolve/main"
)

# filename -> expected number of data rows (excludes header), per canonical Olist
EXPECTED_ROWS = {
    "olist_customers_dataset.csv": 99441,
    "olist_geolocation_dataset.csv": 1000163,
    "olist_order_items_dataset.csv": 112650,
    "olist_order_payments_dataset.csv": 103886,
    "olist_order_reviews_dataset.csv": 99224,
    "olist_orders_dataset.csv": 99441,
    "olist_products_dataset.csv": 32951,
    "olist_sellers_dataset.csv": 3095,
    "product_category_name_translation.csv": 71,
}


def count_rows(path: Path) -> int:
    """
    Number of data rows, excluding the header.

    Uses a real CSV parser (not a line count) because some Olist files — the
    reviews in particular — contain free-text fields with embedded newlines,
    which would inflate a naive line count.
    """
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # skip header
        return sum(1 for _ in reader)


def download(filename: str, dest: Path) -> None:
    url = f"{BASE_URL}/{filename}"
    tmp = dest.with_suffix(dest.suffix + ".part")
    urllib.request.urlretrieve(url, tmp)  # noqa: S310 (trusted host)
    tmp.replace(dest)


def main() -> int:
    raw_dir = Path(os.environ.get("RAW_DATA_DIR", "data/raw"))
    raw_dir.mkdir(parents=True, exist_ok=True)

    failures = []
    for filename, expected in EXPECTED_ROWS.items():
        dest = raw_dir / filename

        if dest.is_file() and count_rows(dest) == expected:
            print(f"[skip]     {filename:45} already present ({expected:,} rows)")
            continue

        print(f"[download] {filename:45} ...", flush=True)
        try:
            download(filename, dest)
        except Exception as exc:  # noqa: BLE001
            print(f"[FAIL]     {filename}: {exc}", file=sys.stderr)
            failures.append(filename)
            continue

        rows = count_rows(dest)
        if rows != expected:
            print(
                f"[FAIL]     {filename}: got {rows:,} rows, expected {expected:,}",
                file=sys.stderr,
            )
            failures.append(filename)
        else:
            size_mb = dest.stat().st_size / 1_048_576
            print(f"[ok]       {filename:45} {rows:,} rows ({size_mb:.1f} MB)")

    if failures:
        print(f"\n{len(failures)} file(s) failed: {failures}", file=sys.stderr)
        return 1

    print(f"\nAll 9 Olist files present and verified in {raw_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
