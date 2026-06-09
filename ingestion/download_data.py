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
import hashlib
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

# filename -> expected SHA-256 of the exact bytes. Row counts alone wouldn't
# catch a tampered/substituted mirror with the same number of rows; the hash
# pins the content. Regenerate after an intentional source change with:
#   sha256sum data/raw/*.csv
EXPECTED_SHA256 = {
    "olist_customers_dataset.csv": "983a422239e1712ded753b3bf9ecf47dc73f144d306029dcfa99e70a226883d2",
    "olist_geolocation_dataset.csv": "b514f6fc991b9566aeba02aa5d67e2c3630f034b60a0e05aa0d082a3b66d88d6",
    "olist_order_items_dataset.csv": "0bc4d068c4fe38cbb01bd90e8746e3c613fe7b4baef75fab7b0e329701c3e279",
    "olist_order_payments_dataset.csv": "4f713964f2815dbbaa40b9488268c55aac3627bfce5aa96cf58d1f3616de3cc0",
    "olist_order_reviews_dataset.csv": "012b61c7593e34f51fa614efdf802b9c7056ce6aae5307ddb93236e7cfc797d7",
    "olist_orders_dataset.csv": "8df58ef3d2d7e9944010f7beecd9b75367f5588ec6e3c91cec19ae3345ef9ecf",
    "olist_products_dataset.csv": "3e6569628a17fbc75fd206ee357b59e20364b9afa90f5b6cd5b4d624c58aa9cc",
    "olist_sellers_dataset.csv": "1f643d2b950373b85735e7794b20986f528d7a000432e7c6f9bcbb44d0846a0e",
    "product_category_name_translation.csv": "a81f0d1f27b27e7293f761bc79e3ce8f348ee39c4b3ed3e49bde38f478586278",
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


def sha256(path: Path) -> str:
    """Streaming SHA-256 of a file (handles the ~58 MB geolocation file)."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify(filename: str, path: Path) -> str | None:
    """Return None if the file matches row count AND checksum, else a reason."""
    rows = count_rows(path)
    if rows != EXPECTED_ROWS[filename]:
        return f"got {rows:,} rows, expected {EXPECTED_ROWS[filename]:,}"
    digest = sha256(path)
    if digest != EXPECTED_SHA256[filename]:
        return f"checksum mismatch ({digest[:12]}…)"
    return None


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

        if dest.is_file() and verify(filename, dest) is None:
            print(f"[skip]     {filename:45} already present ({expected:,} rows, checksum ok)")
            continue

        print(f"[download] {filename:45} ...", flush=True)
        try:
            download(filename, dest)
        except Exception as exc:  # noqa: BLE001
            print(f"[FAIL]     {filename}: {exc}", file=sys.stderr)
            failures.append(filename)
            continue

        reason = verify(filename, dest)
        if reason:
            print(f"[FAIL]     {filename}: {reason}", file=sys.stderr)
            failures.append(filename)
        else:
            size_mb = dest.stat().st_size / 1_048_576
            print(f"[ok]       {filename:45} {expected:,} rows, checksum ok ({size_mb:.1f} MB)")

    if failures:
        print(f"\n{len(failures)} file(s) failed: {failures}", file=sys.stderr)
        return 1

    print(f"\nAll 9 Olist files present and verified in {raw_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
