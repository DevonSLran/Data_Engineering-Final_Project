# Olist ELT Pipeline — Step-by-Step Technical Walkthrough

A complete, grounded explanation of how data physically moves through the pipeline,
and exactly what dbt does at each stage. Every detail below maps to real code in this repo.

---

## The big idea: ELT vs ETL

- **ETL** (old way): transform data *before* it lands in the warehouse — in an external tool.
- **ELT** (your way): **E**xtract → **L**oad raw first → **T**ransform *inside* the warehouse with SQL.

Your transform engine is **dbt**; your warehouse is **PostgreSQL**. The whole thing
is organized into **3 schemas** inside one Postgres database (`olist`):

```
bronze.*    ← raw landing (Python loads this)
staging.*   ← cleaned views (dbt builds this)   = "Silver"
marts.*     ← star schema  (dbt builds this)    = "Gold"
```

---

## STEP 0 — Extract: `ingestion/download_data.py`

The DAG's first task. Downloads the 9 CSVs from the Hugging Face mirror into
`data/raw/`, and **verifies each file twice**: against its known **row count** *and*
a pinned **SHA-256 checksum**. If a file is already present and valid, it's skipped
(idempotent). If a checksum doesn't match, that file fails — so a tampered or
swapped mirror can't slip in.

**Output:** 9 verified CSV files on disk.

---

## STEP 1 — Load (Bronze): `ingestion/load_raw.py`

This is the **L** of ELT. Pure Python + psycopg2, no transformation. Per file:

1. **Reads the CSV header** to get column names (`utf-8-sig` strips the BOM — a hidden
   character some CSVs start with).
2. **`DROP TABLE IF EXISTS ... CASCADE`** then **`CREATE TABLE`** with **every column
   as `TEXT`**. Deliberate — Bronze makes *zero* assumptions about types. A date is
   just text here. This guarantees the load never fails on a malformed value.
3. **`COPY ... FROM STDIN`** — Postgres's bulk-load command. Streams the whole file in
   one shot. Used because the geolocation file is ~1M rows; row-by-row `INSERT`s would
   be painfully slow.
4. Counts the rows and prints a summary.

**Idempotency** comes from drop-and-recreate: run it 100 times, you get the identical
state every time (a "full refresh").

It's also **transactional** — `conn.autocommit = False`, all 9 tables load inside one
transaction. If file 7 fails, `rollback()` undoes everything. You never end up with a
half-loaded warehouse.

**Output:** `bronze.orders`, `bronze.customers`, … 9 tables, all TEXT columns, ~1.55M rows total.

---

## What *is* dbt? (the three things you rely on)

dbt = **data build tool**. You write `SELECT` statements; dbt turns each one into a
table or view in the warehouse and manages the dependencies between them.

### 1. `ref()` and `source()` — the dependency graph
- `{{ source('bronze', 'orders') }}` → "read from the raw `bronze.orders` table loaded
  externally." Bronze tables are declared as *sources*.
- `{{ ref('stg_orders') }}` → "read from my dbt model named `stg_orders`." dbt replaces
  this with the real schema-qualified table name at runtime.

Because every model references others through `ref()`, **dbt builds a dependency graph
(a DAG)** and figures out the correct build order automatically. You never hand-order
them. `dim_date` reads `stg_orders`, so dbt knows it must build `stg_orders` first.

### 2. Materialization — table vs view
Set in `dbt_project.yml`:
```yaml
staging:  +materialized: view    # Silver = views
marts:    +materialized: table   # Gold = tables
```
- A **view** stores no data — a saved query that runs live against Bronze every time it's
  read. Cheap, always fresh. Perfect for the light cleaning in Silver.
- A **table** physically stores the result. Slower to build, fast to query. Right for
  Gold, which Metabase hits repeatedly.

### 3. The custom schema macro (`macros/generate_schema_name.sql`)
By default dbt names schemas `<target>_<custom>` (e.g. `dev_staging`). The macro overrides
that so you get clean names: `staging` and `marts`, not `dev_staging`. The `+schema:`
config drives it.

**Connection** comes from `profiles.yml` — all values via `env_var(...)`, so the *exact
same files* work on your laptop and inside the Airflow container. That's a big part of why
it's reproducible.

---

## STEP 2 — Transform, Silver: `dbt run --select staging`

8 staging models, each materialized as a **view**. `stg_orders.sql` does exactly three jobs:

```sql
with source as (select * from {{ source('bronze', 'orders') }})
select
    order_id,
    order_status,
    order_purchase_timestamp::timestamp  as order_purchase_timestamp,  -- CAST text→timestamp
    ...
from source
```

1. **Cast types** — `::timestamp`, `::numeric`, etc. Text becomes real types.
2. **Rename** — columns get clean, consistent names.
3. **Light joins** — e.g. `stg_products` joins the category-translation table to attach
   English category names.

No data is stored; these are queries that sit live on top of Bronze.

**Output:** `staging.stg_orders`, `staging.stg_order_items`, … 8 typed views.

---

## STEP 3 — Test Silver: `dbt test --select staging`

dbt runs the tests declared in the staging `.yml` files. If any fails, **the task fails
and the DAG stops** — bad data never reaches Gold. First quality gate.

---

## STEP 4 — Transform, Gold: `dbt run --select marts`

The star schema, materialized as **tables**. Four dimensions + one fact.

**`dim_date.sql`** is *generated*, not loaded from source — it finds the min/max order
date, then `generate_series(...)` produces one row per calendar day between them.
`date_key` is a `YYYYMMDD` integer (e.g. `20170815`). 774 days.

**`fct_order_items.sql`** — the center of the star:

- **Grain:** one row per item line in an order. Surrogate key = `order_id || '-' || order_item_id`.
- **Foreign keys** to each dimension: `customer_id`, `product_id`, `seller_id`, and
  `order_purchase_date_key` (the date converted to that same `YYYYMMDD` integer so it
  joins to `dim_date`).
- **Measures:** `price`, `freight_value`, `item_total_value = price + freight`, and two
  derived delivery metrics from timestamp differences (`extract(epoch ...) / 86400` =
  seconds → days).
- Joins `stg_order_items` to `stg_orders` to pull order-level context onto each item line.

### The incremental materialization (the +10% feature)
```sql
{{ config(materialized='incremental', unique_key='order_item_key',
          incremental_strategy='delete+insert') }}
...
{% if is_incremental() %}
  where o.order_purchase_timestamp >= (select max(order_purchase_timestamp) from {{ this }})
{% endif %}
```

- `is_incremental()` is a dbt flag: **false** on the first build / `--full-refresh`,
  **true** on normal runs.
- **First run:** the `where` is skipped → builds all 112,650 rows.
- **Normal daily run:** the `where` kicks in → only processes orders at/after the latest
  timestamp already in the table (`{{ this }}` = the existing fact table). Then
  `delete+insert` swaps those boundary rows.
- Result: re-running is **cheap and idempotent**. On a live feed it'd grab only the new
  day's slice instead of rebuilding 112k rows nightly. On the static dataset it's a no-op
  after the first load — the honest, correct behavior.

**Output:** `marts.fct_order_items` + `marts.dim_customers/products/sellers/date`.

---

## STEP 5 — Test Gold: `dbt test --select marts`

The serious quality gate (`_marts.yml`). Tests are *declarative* — listed in YAML, dbt
generates the SQL:

- **`unique` + `not_null`** on every dimension PK and the fact's `order_item_key` — no
  duplicate keys.
- **`relationships`** on all 4 foreign keys — e.g. every `customer_id` in the fact must
  exist in `dim_customers`. This is **referential integrity**: no orphan rows.
- **`not_null`** on `price`.

Plus `accepted_values` checks elsewhere (order_status, review_score 1–5). **49 tests**
total. Any failure halts the pipeline.

---

## STEP 6 — `dbt docs generate`

Builds the dbt documentation site (`manifest.json` + `catalog.json`) — auto-generated
lineage graph and column-level docs. A required deliverable.

---

## How Airflow ties it together (`container/dags/olist_pipeline.py`)

One DAG, `olist_elt_pipeline`, `@daily` schedule. Each step is a `BashOperator` that
shells out to a command. The chain:

```
extract_raw → load_bronze → dbt_run_staging → dbt_test_staging
            → dbt_run_marts → dbt_test_marts → dbt_docs_generate
```

The `>>` operator sets dependencies: a task only runs if the one before it succeeded. A
failed test **blocks everything downstream** — the safety guarantee, enforced by the
orchestrator, not by hope.

Note `DBT = "/opt/dbt-venv/bin/dbt"` — dbt is called by **absolute path** because it lives
in an isolated virtualenv inside the Airflow image, so its dependencies never clash with
Airflow's.

---

## One line per layer (slide-ready)

| Step | Tool | What happens | Result |
|------|------|--------------|--------|
| Extract | `download_data.py` | fetch + checksum-verify 9 CSVs | files on disk |
| Load (Bronze) | `load_raw.py` + Postgres `COPY` | raw, all-TEXT, idempotent | `bronze.*` |
| Transform (Silver) | dbt views | cast, rename, light joins | `staging.*` |
| Test Silver | dbt | quality gate #1 | pass / stop |
| Transform (Gold) | dbt tables | star schema, incremental fact | `marts.*` |
| Test Gold | dbt | PK + referential integrity (49 tests) | pass / stop |
| Docs | dbt | lineage + column docs | docs site |
| *All of it* | Airflow | ordered, scheduled, halts on failure | green DAG |

**The mental model to repeat in the room:** *"Land it raw, clean it in views, model it
into a tested star schema — and let Airflow run the whole chain in order, stopping the
moment a test fails."*
