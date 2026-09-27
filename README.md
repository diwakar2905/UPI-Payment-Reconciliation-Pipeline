# UPI Payment Reconciliation Pipeline

An end-to-end batch data engineering pipeline designed to reconcile UPI transactions across three disparate systems (Merchant App Database, Payment Gateway, and Bank Settlement Files) and flag financial discrepancies.

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
└── requirements.txt            # Python dependencies
```
