# UPI Payment Reconciliation Pipeline

[![CI](https://github.com/diwakar2905/UPI-Payment-Reconciliation-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/diwakar2905/UPI-Payment-Reconciliation-Pipeline/actions/workflows/ci.yml)

An end-to-end batch data engineering pipeline designed to reconcile UPI transactions across three disparate systems (Merchant App Database, Payment Gateway, and Bank Settlement Files) and flag financial discrepancies.

## The problem

A UPI merchant's money moves through three systems that never agree with each other out of the box: the merchant app's own order records, the payment gateway's event log, and the bank's daily settlement file. Money silently goes missing between them - a payment succeeds at the gateway but the bank never settles it, a settlement arrives days late, a customer gets charged twice, a refund never gets reconciled against its original payment. Finding these by hand across three CSVs and a database is slow and error-prone. This pipeline generates realistic (synthetic) versions of all three systems, deliberately injects the discrepancies that show up in production, and then reconciles them automatically, flagging every mismatch with a specific reason.

## Architecture Overview

```
                      +-------------------+
                      |   Synthetic Data  |
                      |     Generator     |
                      +---------+---------+
                                |
             +------------------+------------------+
             |                  |                  |
             v                  v                  v
     +---------------+  +---------------+  +---------------+
     |  app.orders   |  | gateway CSVs  |  |settlement CSVs|
     | (PostgreSQL)  |  |  (daily raw)  |  |  (daily raw)  |
     +-------+-------+  +-------+-------+  +-------+-------+
             |                  |                  |
             +------------------+------------------+
                                |
                                v
               [ Python Extraction & Validation ]
                                |
                 +--------------+--------------+
                 | (Valid rows)                | (Invalid rows)
                 v                             v
           +-----------+                 +--------------+
           | raw.* DB  |                 | raw.rejected |
           +-----+-----+                 +--------------+
                 |
                 v
         [ dbt Transformation ]
          ├── Staging (Views)
          ├── Snapshots (SCD Type 2 Merchants)
          ├── Marts (fact_reconciliation, dim_date, etc.)
          └── Data Quality & Accuracy Tests
                 |
                 v
        +------------------+
        | Dashboards & BI  |
        | (Metabase/PowerBI|
        +------------------+
```

## Tech Stack
- **Languages & Frameworks:** Python 3.11+, SQL
- **Database:** PostgreSQL 16
- **Data Modeling & Transformation:** dbt (dbt-core, dbt-postgres)
- **Data Quality & Testing:** dbt tests, pytest
- **Orchestration:** Apache Airflow 2.x
- **Containerization & CI/CD:** Docker Compose, GitHub Actions

## Repository Layout
```
├── airflow/
│   └── dags/                   # Airflow DAG definitions
├── dashboard/
│   └── screenshots/            # BI dashboards and reconciliation visuals
├── dbt_upi/
│   ├── models/
│   │   ├── staging/            # Staging cleaning and deduplication
│   │   └── marts/              # Star schema & reconciliation fact models
│   ├── snapshots/              # SCD Type 2 dimension snapshots
│   ├── seeds/                  # Answer key for ground-truth validation
│   └── tests/                  # Custom dbt assertions
├── docs/                       # Architecture diagrams and specifications
├── generator/
│   ├── config.yaml             # Generator configuration & error injection rates
│   └── generate_data.py        # Seeded multi-system transaction simulator
├── ingestion/
│   ├── db.py                   # Database connection manager
│   ├── extract.py              # Incremental extract module
│   ├── validate.py             # Schema & data contract validator
│   └── load.py                 # Idempotent raw loading module
├── sql/
│   └── init.sql                # Schemas and DDL for app, raw, staging, marts
├── tests/                      # pytest test suite for generator & ingestion
├── docker-compose.yml          # Local containerized infrastructure
├── Makefile                    # Developer workflow automation
├── requirements.in             # Direct Python dependencies (loose bounds)
└── requirements.txt            # Pinned lockfile, generated via `make lock`
```

## Run steps

```bash
git clone <this repo> && cd UPI-Payment-Reconciliation-Pipeline
cp .env.example .env               # edit PG_PASSWORD etc if needed

make docker-up                     # starts Postgres + Metabase
make setup                         # pip install -r requirements.txt (pinned)
make init-db                       # creates app/raw/staging/marts schemas

make generate                      # 10 days of synthetic orders/gateway/settlement data
make run DATE=2026-09-01           # extract -> validate -> load -> dbt snapshot/run/test, one day
make backfill START=2026-09-01 END=2026-09-10   # ...or all of them at once

make test                          # pytest: generator determinism, validator contracts
```

`make dbt-seed` (re)loads the generator's `answer_key.csv` ground truth into
`staging.answer_key` - needed for the accuracy tests/model below, and only
valid until the next `make generate` overwrites the seed file.

Airflow (`airflow/dags/upi_reconciliation_dag.py`) runs the same
extract→validate→load→dbt chain on a daily schedule instead of by hand;
point `AIRFLOW_HOME`/dags folder at this repo and it picks it up.

## Design decisions

**Why a dbt snapshot for merchants, not a plain SCD2 table?** Merchant fee
rates change over time (renegotiated MDR), and `fact_reconciliation`'s MDR
fee calculation has to use the rate that was in effect *when the order was
placed*, not today's rate. A dbt snapshot (`merchants_snapshot`, timestamp
strategy on `updated_at`) gives us that history for free -
`dbt_valid_from`/`dbt_valid_to` per row - instead of a hand-rolled trigger
or application-level versioning table.

**How idempotency works.** Every ingestion step is keyed by `--run-date`.
`load.py` does `DELETE FROM raw.<table> WHERE run_date = %s` immediately
before its insert, in the same transaction, so re-running `make run
DATE=2026-09-01` a hundred times in a row leaves exactly the same rows in
`raw.*` as running it once. `fact_reconciliation` (see below) uses the same
idea one layer up: it's an incremental model keyed on `order_id`, so
reprocessing a date doesn't create duplicate reconciliation rows either.

**Why `fact_reconciliation` is incremental, and how "late data" is
handled.** A naive incremental model that only looks at "orders created on
today's run_date" would miss the entire point of T+1 settlement and
refunds: an order created on day 3 can have its settlement arrive on day 4
and its refund arrive on day 6-7. So the incremental filter isn't "new
orders" - it's "any order whose `app.orders`, gateway, *or settlement* row
was loaded with `run_date` = today", computed by unioning the order IDs
touched across all three raw tables for that run_date. Everything else in
the table is left untouched (`delete+insert` on `order_id`). This is also
why `assert_reconciliation_matches_answer_key` and
`accuracy_by_error_type` only score orders old enough to have had time to
fully settle (a 5-day maturity buffer, measured from the newest run_date
actually loaded, not wall-clock time) - the newest few days in any given
run are *supposed* to look unsettled, that's not a bug.

**T+1 settlement and refunds as first-class, not edge cases.** Real UPI
settlement lags the payment by one business day; this is the default in
the generator (`settled_date = run_date + 1`), not something bolted on via
the `late_settlement` error - `late_settlement` pushes it to T+3..T+6
specifically so it's distinguishable from normal settlement lag.
`fact_reconciliation`'s `is_late_settlement` flag is measured against
`gateway_event_time + 1 day`, so T+1 itself is never flagged. Refunds are
modeled as a second settlement row (`settlement_type = 'refund'`) against
the same `txn_id`, with a negative `gross_amount`/`net_settled` and no fee
reversal (real MDR/GST typically isn't refunded) - not a mutation of the
original payment settlement, so both the original charge and its reversal
stay auditable.

## Measured numbers

From a local 9-day / ~2,700-order run (`make generate --days 9
--orders-per-day 300`, seed 42 - fully reproducible, see `generator/config.yaml`):

| Metric | Value | Source |
|---|---|---|
| Payments reconciled | 2,709 (2,125 matched, 503 discrepancy, 75 refunded, 6 still pending) | `staging_marts.fact_reconciliation` |
| Invalid row rate | 0.55% (41 of 7,413 raw rows rejected) | `raw.rejected` vs total rows loaded |
| Avg. detector precision | 0.85 across 5 scored error types | `staging_marts.accuracy_by_error_type` |
| Avg. detector recall | 0.99 across 5 scored error types | `staging_marts.accuracy_by_error_type` |
| Ingestion throughput | ~1.3s total extract+validate+load time for 9 days | `raw.pipeline_runs` |

"Speedup vs. manual reconciliation" isn't included above because there's no
manual baseline to measure it against in this repo - if you're citing this
project, measure your own team's manual/spot-check time on an equivalent
volume and compare it to the ingestion throughput number above, rather than
quoting a number this repo can't back up.

Re-run these yourself: `make dbt-run && make dbt-test` then query
`staging_marts.accuracy_by_error_type` and `staging_marts.daily_reconciliation_summary`
directly, or open them in Metabase (`docs/dashboard.md`).
