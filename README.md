# Olist Medallion ELT Pipeline

An end-to-end, containerized **ELT pipeline** on the Brazilian E-Commerce
(Olist) dataset, built on a three-layer **medallion architecture** and
orchestrated by **Apache Airflow**.

Raw CSVs are loaded into PostgreSQL (Bronze), cleaned and typed with dbt
(Silver), and modeled into a **star schema** (Gold) that powers Metabase
dashboards — the whole flow runs on a schedule, end-to-end, inside Docker.

| | |
|---|---|
| **Warehouse** | PostgreSQL 16 |
| **Transformations** | dbt 1.8 (`dbt-postgres`) |
| **Orchestration** | Apache Airflow 3.0.1 (LocalExecutor) |
| **BI** | Metabase |
| **Packaging** | Docker Compose |

---

## Architecture

```mermaid
flowchart LR
    subgraph SRC["Source"]
        CSV["9 Olist CSVs<br/>data/raw/"]
    end

    subgraph PG["PostgreSQL warehouse"]
        direction TB
        B["<b>Bronze</b><br/>bronze.*<br/>raw, all TEXT"]
        S["<b>Silver</b><br/>staging.*<br/>typed views"]
        G["<b>Gold</b><br/>marts.*<br/>star schema"]
        B --> S --> G
    end

    CSV -->|"load_raw.py<br/>(Python COPY)"| B
    G --> MB["Metabase<br/>dashboards"]

    subgraph AF["Apache Airflow — olist_elt_pipeline (daily)"]
        T0["extract_raw"] --> T1["load_bronze"] --> T2["dbt_run_staging"] --> T3["dbt_test_staging"] --> T4["dbt_run_marts"] --> T5["dbt_test_marts"] --> T6["dbt_docs_generate"]
    end

    AF -.orchestrates.-> PG
```

The pipeline is **ELT, not ETL**: data lands raw first, then every
transformation happens *inside* the warehouse with dbt. Each medallion layer
adds trust:

| Layer | Schema | What it is | Materialization |
|-------|--------|------------|-----------------|
| 🥉 **Bronze** | `bronze` | Exact copy of source CSVs, all columns `TEXT`, no logic | tables (full refresh) |
| 🥈 **Silver** | `staging` | Cleaned, renamed, correctly-typed | views |
| 🥇 **Gold** | `marts` | Star schema (fact + dimensions) for BI | tables |

---

## Dataset

The **[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**
— ~100K orders placed between 2016–2018 across multiple Brazilian
marketplaces, spread over 9 related CSV files.

> **Data provenance:** the canonical source is Kaggle (link above). For this
> build the files were pulled from a public **Hugging Face mirror**
> (`aviahYadler/Olist_Ecommerce_Dataset`) because the build environment had no
> Kaggle credentials. `download_data.py` verifies every file against both its
> canonical Olist **row count** and a pinned **SHA-256 checksum**, so a
> tampered or swapped mirror can't slip through unnoticed.

| File | Bronze table | Rows |
|------|--------------|------|
| `olist_orders_dataset.csv` | `orders` | 99,441 |
| `olist_customers_dataset.csv` | `customers` | 99,441 |
| `olist_order_items_dataset.csv` | `order_items` | 112,650 |
| `olist_order_payments_dataset.csv` | `order_payments` | 103,886 |
| `olist_order_reviews_dataset.csv` | `order_reviews` | 99,224 |
| `olist_products_dataset.csv` | `products` | 32,951 |
| `olist_sellers_dataset.csv` | `sellers` | 3,095 |
| `olist_geolocation_dataset.csv` | `geolocation` | 1,000,163 |
| `product_category_name_translation.csv` | `product_category_name_translation` | 71 |
| | **Total** | **1,550,922** |

---

## Project structure

```
DATA_ENG_ALP/
├── docker-compose.yml          # the full stack (warehouse, airflow, metabase)
├── .env.example                # config template (copy to .env)
├── container/
│   ├── airflow.Dockerfile      # Airflow image + isolated dbt venv
│   ├── requirements.txt        # ingestion deps (psycopg2) for Airflow's env
│   ├── dbt-requirements.txt    # fully-pinned dbt lockfile
│   └── dags/
│       └── olist_pipeline.py   # the Airflow DAG
├── ingestion/
│   ├── download_data.py        # Extract: fetch + verify the 9 CSVs (row count + SHA256)
│   └── load_raw.py             # Bronze loader (idempotent COPY)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            # connection via env vars
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/            # Silver: 8 stg_*.sql + tests (_staging.yml)
│       └── marts/              # Gold: dim_*/fct_* + tests (_marts.yml)
├── data/raw/                   # the 9 Olist CSVs (gitignored)
└── credentials/
```

---

## Quickstart

**Prerequisites:** Docker + Docker Compose, Python 3 (only for the data-download
helper), and an internet connection on first run.

```bash
# 1. Get the raw data (downloads the 9 Olist CSVs into data/raw/, verifies them)
python ingestion/download_data.py

# 2. Configure  (macOS / Linux)
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env     # so bind-mounted logs stay writable

# 3. Launch the whole stack
docker compose up -d

# 4. Open the UIs
#    Airflow   → http://localhost:8000   (admin / admin)
#    Metabase  → http://localhost:3000   (first-run setup wizard)

# 5. Run the pipeline
#    In Airflow: enable the `olist_elt_pipeline` DAG and click ▶ Trigger.
#    All 7 tasks complete end-to-end in ~1–2 minutes.
```

> **Windows (PowerShell)** — steps 1, 3, 4 and 5 are identical; only the
> configure step differs (no `cp` / `id -u`):
>
> ```powershell
> # 2. Configure
> Copy-Item .env.example .env
> # AIRFLOW_UID is only needed for Linux bind-mount permissions. On Docker
> # Desktop (Windows/macOS) the default 50000 already works, so leave .env as-is.
> ```

> The data is **not** committed to git (it's large and gitignored), so step 1
> is required on a fresh clone. The download script is idempotent — files
> already present with a matching row count **and** SHA-256 checksum are skipped.

> **Ports note:** this project intentionally uses non-default host ports
> (warehouse `5442`, Airflow `8000`) to avoid clashing with anything already
> bound to the usual `5432` / `8080`. All values live in `.env`.

---

## Pipeline walkthrough

### 🥉 Bronze — `ingestion/load_raw.py`

A pure-Python loader that `COPY`s each CSV into the `bronze` schema with **all
columns as `TEXT`** — a faithful, untransformed copy of the source. It is
**idempotent**: every run drops and recreates each table, so re-running always
yields the same state (a full refresh). `COPY` is used for speed (the
geolocation file alone is ~1M rows).

### 🥈 Silver — `dbt/models/staging/`

Eight dbt **views** (no data stored) that sit on top of Bronze and do three
things: **cast types**, **rename** columns, and **join** trivial lookups
(products gets its English category name here).

```
stg_orders   stg_order_items   stg_customers   stg_sellers
stg_products   stg_order_payments   stg_order_reviews   stg_geolocation
```

### 🥇 Gold — `dbt/models/marts/`

A classic **star schema** materialized as physical tables — one fact at the
order-item grain, surrounded by four conformed dimensions. `fct_order_items` is
materialized **`incremental`** (on `order_purchase_timestamp`, keyed by
`order_item_key`): a `--full-refresh` builds every row, while a normal daily run
only (re)loads orders at/after the high-water mark and `delete+insert`s them —
so re-running the same day is idempotent and cheap.

```mermaid
erDiagram
    dim_customers ||--o{ fct_order_items : customer_id
    dim_products  ||--o{ fct_order_items : product_id
    dim_sellers   ||--o{ fct_order_items : seller_id
    dim_date      ||--o{ fct_order_items : order_purchase_date_key

    fct_order_items {
        text order_item_key PK
        text order_id
        int order_item_id
        text customer_id FK
        text product_id FK
        text seller_id FK
        int order_purchase_date_key FK
        text order_status
        numeric price
        numeric freight_value
        numeric item_total_value
        numeric delivery_days
        numeric delivery_vs_estimate_days
    }
    dim_customers {
        text customer_id PK
        text customer_unique_id
        text customer_city
        text customer_state
    }
    dim_products {
        text product_id PK
        text product_category
        text product_category_pt
    }
    dim_sellers {
        text seller_id PK
        text seller_city
        text seller_state
    }
    dim_date {
        int date_key PK
        date date
        int year
        int month
        bool is_weekend
    }
```

| Table | Grain | Rows |
|-------|-------|------|
| `fct_order_items` | one item line per order | 112,650 |
| `dim_customers` | customer_id (per-order key) | 99,441 |
| `dim_products` | product_id | 32,951 |
| `dim_sellers` | seller_id | 3,095 |
| `dim_date` | one calendar day | 774 |

---

## Orchestration

The Airflow DAG **`olist_elt_pipeline`** runs the whole flow on a `@daily`
schedule, one task after another — **extract and load included**, so the entire
ingestion-to-docs path is orchestrated, not just the transforms. If any
data-quality test fails, the run stops — bad data never reaches Gold.

```
extract_raw → load_bronze → dbt_run_staging → dbt_test_staging
            → dbt_run_marts → dbt_test_marts → dbt_docs_generate
```

- **`extract_raw`** — runs `download_data.py` (idempotent: skips files already
  present with the right row count + checksum). Needs outbound internet only on
  the first run.
- **`load_bronze`** — full-refresh COPY into `bronze`.
- **`dbt_run/test_*`** — build + test Silver, then Gold.
- **`dbt_docs_generate`** — builds the dbt docs site (`manifest.json` + `catalog.json`).

![Airflow DAG — olist_elt_pipeline, all 7 tasks green](img/airflow_Dag.png)

---

## Data quality

Tests are defined in dbt (`_staging.yml`, `_marts.yml`) and run as dedicated
DAG tasks. **49 data tests** across the Silver and Gold layers, all passing:

- **Primary keys** — `unique` + `not_null` on every dimension PK and the fact's surrogate key.
- **Referential integrity** — `relationships` tests on all four foreign keys from `fct_order_items` to its dimensions.
- **Domain checks** — `accepted_values` on `order_status` (8 valid states), `payment_type`, and `review_score` (1–5).
- **Documented quirks** — `review_id` is intentionally *not* tested for uniqueness (the Olist source legitimately repeats it across orders).

```bash
# run all transformations + tests manually (outside Airflow)
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

---

## dbt documentation

The DAG's final task (`dbt_docs_generate`) builds the dbt docs site
(`target/manifest.json` + `catalog.json`) on every run. To browse the
auto-generated model/column lineage and descriptions locally:

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt docs generate \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt docs serve \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --port 8080
```

---

## Dashboards (Metabase)

Built on the `marts` (Gold) schema. Three charts answer concrete business
questions.

> **Metabase connection note:** Metabase runs *inside* the Docker network, so
> connect it to the warehouse using host **`warehouse`** and port **`5432`**
> (the internal port) — *not* `localhost:5442`.

<!-- ============================================================ -->
<!-- TODO (teammate): Metabase dashboards + screenshots           -->
<!-- ============================================================ -->

### 1. Revenue by Product Category

![Revenue by Product Category](img/Revenue_by_Product_Category.png)

> 📸 Revenue distribution by product category.
### 2. Monthly Revenue Trend

![Monthly Revenue Trend](img/Monthly_Revenue_Trend.png)

> 📸 Monthly revenue trend analysis.
### 3. Delivery Performance by State

![Delivery Performance by State](img/Delivery_Performance_by_State.png)

> 📸 Delivery performance comparison across states.
---

## Design decisions & notes

- **"Daily" on a static dataset.** The Olist source is a fixed 2016–2018
  historical dump, so there is no genuinely "new" data each day. The pipeline
  is honest about this: Bronze is a daily **full refresh** (drop + reload), and
  `fct_order_items` loads **incrementally** on `order_purchase_timestamp`. Both
  make re-running a given day **idempotent** — the same input always yields the
  same warehouse state — which is the property the daily schedule is meant to
  demonstrate. On a live feed the incremental fact would pick up only the new
  slice; here it's a no-op after the first load.
- **dbt in an isolated venv.** dbt is installed into its own
  `/opt/dbt-venv` inside the Airflow image (from a fully-pinned
  `dbt-requirements.txt` lockfile) so its dependencies never collide with
  Airflow's pinned constraint set. The DAG calls it by absolute path.
- **`dim_customers` grain.** It's keyed on `customer_id`, which in Olist is
  generated **per order** (one row per order, 99,441 total) — not on the real
  person. That's deliberate: the fact only carries the order-scoped
  `customer_id`, so the dimension must match that grain to join. The true-person
  key, `customer_unique_id`, is carried through as an attribute, so unique-buyer
  counts are still one `count(distinct …)` away.
- **Geolocation & reviews excluded from Gold.** Both are available in
  Bronze/Silver but don't fit the order-item grain cleanly (geolocation has
  ~1M rows with no clean FK; reviews repeat `review_id`), so they're kept out
  of the star schema.
- **Airflow 3 specifics.** v3 splits the API server, scheduler, and
  dag-processor into separate services and requires
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` and a shared
  `AIRFLOW__API_AUTH__JWT_SECRET` across containers for task execution to work.
- **Production hardening (not built).** This is a course project: tasks use
  `retries: 1` and there's no alerting. For production I'd add an
  `on_failure_callback` (Slack/email), more aggressive retries with backoff,
  SLAs on the DAG, and source-freshness checks via `dbt source freshness`.

---

## Reference

### Services & ports

| Service | Container | Host URL / port | Credentials |
|---------|-----------|-----------------|-------------|
| Airflow UI | `olist_airflow_apiserver` | http://localhost:8000 | `admin` / `admin` |
| Metabase | `olist_metabase` | http://localhost:3000 | set on first visit |
| Warehouse (Postgres) | `olist_warehouse` | `localhost:5442` | `olist` / `olist` (db `olist`) |
| Airflow metadata DB | `olist_airflow_db` | internal | `airflow` / `airflow` |

### Common commands

```bash
docker compose ps                 # service status
docker compose logs -f <service>  # tail a service
docker compose down               # stop (keep data)
docker compose down -v            # stop + wipe all data volumes
```

### Connect to the warehouse directly

```bash
docker exec -it -e PGPASSWORD=olist olist_warehouse psql -U olist -d olist
# then: \dn (schemas)  \dt marts.* (gold tables)
```
