# Olist Medallion ELT Pipeline — Presentation Script

**Format:** 3 presenters · target ~12–15 min talk + ~3–5 min Q&A
**One-line pitch:** *Raw Olist CSVs → PostgreSQL → dbt-modeled star schema → Metabase, fully orchestrated by Airflow, reproducible from a single `docker compose up`.*

> **How to use this:** each presenter owns one section. Read the **[SAY]** lines out
> loud (or paraphrase — don't read robotically). **[SHOW]** tells you what to have on
> screen. **[POINT]** is what to gesture at. Hand-offs are marked **→ HAND OFF**.

---

## Roles at a glance

| Presenter | Owns | Slides / screen | ~Time |
|-----------|------|-----------------|-------|
| **P1 — The Why & The Shape** | Problem, dataset, medallion + ELT architecture | README top + architecture diagram | ~4 min |
| **P2 — The Build** | Bronze loader, Silver dbt views, Gold star schema, data quality | dbt models + ER diagram | ~5 min |
| **P3 — Run It & Prove It** | Airflow orchestration, Docker reproducibility, dashboards, design decisions, **live demo** | Airflow UI + Metabase | ~5 min |

---

# PRESENTER 1 — The Why & The Shape (~4 min)

### Opening
**[SHOW]** README title / a title slide.
**[SAY]** "We built an **end-to-end ELT data pipeline** on the Brazilian
E-Commerce dataset from Olist. The goal: take messy raw CSVs and turn them into
clean, trustworthy, analytics-ready tables that power business dashboards — and
make the whole thing run on a schedule, automatically, inside Docker."

### The dataset
**[SHOW]** the Dataset table in the README.
**[SAY]** "The data is real: about **100,000 orders** placed between **2016 and
2018** across Brazilian marketplaces. It comes as **9 related CSV files** — orders,
customers, order items, payments, reviews, products, sellers, geolocation, and a
category-name translation table. Altogether about **1.5 million rows**."
**[SAY]** "It's relational and a bit messy — exactly the kind of source data a
data engineer has to tame."

### ELT, not ETL
**[SAY]** "A key design choice up front: this is **ELT, not ETL**. We **Extract**
and **Load** the raw data *first*, untouched, and only **Transform** it later —
*inside* the warehouse. That means our raw layer is always a faithful copy of the
source, and every transformation is version-controlled SQL we can rebuild from
scratch."

### The medallion architecture
**[SHOW]** the architecture mermaid diagram.
**[POINT]** at the three layers in the Postgres box.
**[SAY]** "We structured the warehouse in **three layers** — the **medallion
architecture**. Each layer adds trust:"
- **[SAY]** "🥉 **Bronze** — the raw landing zone. An exact copy of the CSVs, every
  column stored as text, *zero* logic. If we ever doubt our numbers, we can always
  come back here to the source of truth."
- **[SAY]** "🥈 **Silver** — cleaned and typed. We fix data types, rename columns
  to be readable, and do light joins. These are dbt **views**, so they store no
  data — they're always live on top of Bronze."
- **[SAY]** "🥇 **Gold** — the business layer. A **star schema**: one fact table
  surrounded by dimensions, ready for BI tools to query."
**[SAY]** "Data flows strictly one direction: Bronze → Silver → Gold. And the
whole chain is orchestrated by **Apache Airflow**, which my teammate will cover."

**[SAY]** "The stack, quickly: **PostgreSQL** as the warehouse, **dbt** for
transformations, **Airflow** for orchestration, **Metabase** for dashboards, all
wrapped in **Docker Compose**."

**→ HAND OFF:** "So that's the shape of the system. [P2] will walk through how
each layer is actually built."

---

# PRESENTER 2 — The Build (~5 min)

### Bronze — the loader
**[SHOW]** `ingestion/load_raw.py` (or just describe it).
**[SAY]** "Let's go layer by layer. **Bronze** is loaded by a small Python script,
`load_raw.py`. It uses Postgres `COPY` to bulk-load each CSV — that's important
because the geolocation file alone is **a million rows**, and `COPY` is far faster
than row-by-row inserts."
**[SAY]** "The loader is **idempotent**: every run **drops and recreates** each
table. So no matter how many times you run it, you get the exact same result — a
clean full refresh. That idempotency is a theme you'll see throughout the
pipeline."
**[SAY]** "Everything lands as **TEXT** here — we deliberately don't type or clean
anything yet. Bronze's only job is to be a faithful mirror of the source."

### Silver — dbt staging views
**[SHOW]** the `staging/` model list.
**[SAY]** "**Silver** is where dbt takes over. We have **eight staging models** —
one per source table — and they're materialized as **views**, so they store no
data and always reflect the latest Bronze. Each one does three things: **cast
types** (text → dates, numbers, booleans), **rename** columns to clear names, and
do **trivial lookups** — for example, products get their English category name
joined in here from the translation table."

### Gold — the star schema
**[SHOW]** the ER diagram in the README.
**[POINT]** at the fact in the center, dimensions around it.
**[SAY]** "**Gold** is the payoff: a classic **star schema**, materialized as real
physical tables for fast BI queries."
**[SAY]** "At the center is the fact table, **`fct_order_items`**. Its **grain** —
the thing one row represents — is **a single item line within an order**. So an
order with three items is three rows. That's 112,650 rows."
**[POINT]** at each dimension.
**[SAY]** "Around it are **four dimensions**: **customers**, **products**,
**sellers**, and a **date** dimension. Each connects to the fact through a foreign
key. The fact holds the **measures** — price, freight, item total, and derived
delivery metrics like actual delivery days versus the estimate."

### Incremental fact (the clever bit)
**[SAY]** "One detail we're proud of: the fact is materialized **incrementally**.
A full-refresh builds all 112,650 rows, but a normal daily run only processes
orders **at or after the latest timestamp already loaded** — using a
`delete+insert` strategy keyed on a surrogate `order_item_key`. So daily re-runs
are **cheap and idempotent** instead of rebuilding everything. On a live data feed
this would pick up just the new slice each day."

### A design decision worth calling out
**[SAY]** "A nuance on the customer dimension: in Olist, `customer_id` is generated
**per order**, not per person — so `dim_customers` has 99,441 rows, one per order.
That's deliberate: the fact only carries that order-scoped key, so the dimension
has to match its grain to join cleanly. The *true* person identifier,
`customer_unique_id`, is carried along as an attribute, so true unique-buyer counts
are still one `count(distinct)` away."

### Data quality
**[SHOW]** the Data quality section.
**[SAY]** "None of this is trustworthy without testing — so we have **49 automated
data tests** built into dbt, and they run as their own pipeline steps:"
- **[SAY]** "**Primary keys** — unique and not-null on every dimension key and the
  fact's surrogate key."
- **[SAY]** "**Referential integrity** — relationship tests confirming every
  foreign key in the fact actually exists in its dimension. No orphan rows."
- **[SAY]** "**Domain checks** — order status only takes its 8 valid values,
  review scores are 1 to 5, and so on."
**[SAY]** "And critically — if **any test fails, the pipeline stops**. Bad data
never reaches the Gold layer or the dashboards."

**→ HAND OFF:** "So that's how the data is built and validated. [P3] will show how
it all runs automatically — and demo it live."

---

# PRESENTER 3 — Run It & Prove It (~5 min)

### Orchestration with Airflow
**[SHOW]** the Airflow DAG screenshot, then the live Airflow UI at `localhost:8000`.
**[SAY]** "Everything you've heard so far is tied together by **Apache Airflow**.
We built one DAG, **`olist_elt_pipeline`**, on a **daily** schedule. It runs
**seven tasks** in order:"
**[POINT]** along the chain.
**[SAY]** "`extract_raw` → `load_bronze` → run and test Silver → run and test Gold
→ generate dbt docs."
**[SAY]** "Notice that **extract and load are *inside* the DAG** — so it's not just
the transforms that are orchestrated, it's the *entire* path from downloading the
raw files to producing documentation. If a test task fails mid-run, the downstream
tasks don't execute — that's the safety gate."
**[SAY]** "We're running **Airflow 3.0.1**, the current major version — which
splits the API server, scheduler, and DAG processor into separate services. Each
runs as its own container."

### Reproducibility — the part graders care about
**[SHOW]** the Quickstart section.
**[SAY]** "A pipeline nobody else can run is worthless, so we put real effort into
**reproducibility**. The entire stack — Postgres, Airflow's three services, and
Metabase — comes up from a **single `docker compose up`**."
**[SAY]** "dbt is pinned to an exact version inside its own isolated virtual
environment, so its dependencies never collide with Airflow's. The data download
is verified against **both a row count and a SHA-256 checksum** — so a tampered or
swapped data mirror can't slip through. We tested this from a clean clone: wipe
everything, one command, and it comes back up green."

### Dashboards (Metabase)
**[SHOW]** Metabase / dashboard screenshots.
**[SAY]** "The Gold star schema feeds **Metabase**, where we built dashboards
answering concrete business questions:"
- **[SAY]** "**Revenue by product category** — what sells."
- **[SAY]** "**Monthly revenue trend** — how the business grew over 2016–2018."
- **[SAY]** "**Delivery performance by state** — an operations view of where
  shipping is slow."
> *(If the teammate's dashboards are ready, walk through them live. If not, describe
> the three questions and note they're built on the `marts` schema.)*

### LIVE DEMO (~90 seconds — optional but high-impact)
**[DO]** Have this ready *before* you present (stack already up):
1. **[SHOW]** Airflow UI → the DAG → click **Trigger**. "I'll kick off a run now."
2. **[SAY]** "Watch the tasks go green one by one — extract, load, the dbt builds,
   the tests, then docs. End to end this takes **one to two minutes**."
3. **[SHOW]** While it runs, switch to the warehouse or Metabase to show real rows.
4. **[SAY]** "And there it is — 112,650 fact rows, all tests passing, dashboards
   live. Fully automated, fully reproducible."
> ⚠️ **Demo safety:** have the stack **already running and warmed up**. If the live
> trigger is risky, show a **previous successful green run** instead — same story,
> zero risk.

### Close
**[SAY]** "To wrap up: we built a **production-shaped ELT pipeline** — medallion
architecture, a tested star schema, full Airflow orchestration, and one-command
reproducibility. It takes raw Olist CSVs and turns them into trustworthy analytics
the business can actually use. Thanks — happy to take questions."

---

# Q&A — anticipated questions + crisp answers

**Q: Why ELT instead of ETL?**
A: Loading raw first means Bronze is always a faithful copy of the source, and every
transformation is version-controlled SQL we can rebuild from scratch. It also lets
the warehouse do the heavy lifting at scale, which Postgres is good at.

**Q: Why a star schema and not just one big table?**
A: A star schema separates *measures* (the fact) from *descriptive context* (the
dimensions). It's the standard BI model — fast to query, easy for analysts to
understand, and it avoids repeating customer/product/seller text on every row.

**Q: What does "incremental" actually buy you on a static dataset?**
A: On this fixed historical dump it's a no-op after the first load — but it proves
the *pattern*. On a live feed, the daily run would process only orders past the
high-water mark instead of rebuilding 112k rows every night. It also keeps re-runs
idempotent and cheap.

**Q: Why is `dim_customers` the same row count as orders?**
A: Olist's `customer_id` is generated per *order*, not per person. The fact carries
that order-scoped key, so the dimension matches that grain to join. The real person
key, `customer_unique_id`, is kept as an attribute for unique-buyer counts.

**Q: How do you know the data is correct?**
A: 49 dbt tests — primary-key uniqueness, not-null, referential integrity on all
foreign keys, and domain checks on categorical fields. They run as pipeline tasks,
and a failure halts the run before bad data reaches Gold.

**Q: Why did you exclude geolocation and reviews from Gold?**
A: Geolocation is ~1M rows with no clean foreign key to the order grain, and reviews
legitimately repeat their `review_id`. Neither fits the order-item star cleanly, so
we keep them in Bronze/Silver but out of the fact model.

**Q: Why pin dbt in its own virtualenv?**
A: Airflow and dbt have conflicting dependency constraints. Isolating dbt in
`/opt/dbt-venv` from a fully-pinned lockfile means the two never collide, and builds
are reproducible.

**Q: Is it actually reproducible by someone else?**
A: Yes — single `docker compose up` brings up the whole stack, the download is
checksum-verified, and we tested it from a clean clone with all volumes wiped.

**Q: What would you add for production?**
A: Failure alerting (`on_failure_callback` to Slack/email), retries with backoff,
DAG SLAs, and `dbt source freshness` checks. We kept it to `retries: 1` and no
alerting since it's a course project.

**Q: Why non-default ports (5442 / 8000)?**
A: To avoid clashing with anything already bound to the usual 5432 / 8080 on the host
— all the port values live in `.env`.

---

# Pre-flight checklist (do this 30 min before)

- [ ] `docker compose ps` — all services **up/healthy**.
- [ ] Airflow UI loads at `localhost:8000`, login `admin` / `admin`.
- [ ] The DAG `olist_elt_pipeline` is **unpaused** and shows a **recent green run**.
- [ ] Metabase reachable at `localhost:3000` (or screenshots ready as backup).
- [ ] README open to the **architecture** and **ER** diagrams.
- [ ] Screenshots saved locally as a **fallback** if live demo fails.
- [ ] Decide who drives the screen during the demo.
- [ ] Each presenter has skimmed the *other* two sections (for Q&A overlap).
```
