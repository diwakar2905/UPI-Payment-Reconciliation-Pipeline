# Architecture

See the top-level `README.md` for the high-level diagram and repository layout.

## Pipeline stages

1. **Generation** (`generator/generate_data.py`) — a seeded simulator produces
   three independent, deliberately inconsistent systems: `app.orders` /
   `app.merchants` in Postgres, `data/<date>/gateway.csv`, and
   `data/<date>/settlement.csv`. It also writes `dbt_upi/seeds/answer_key.csv`,
   the ground truth used by the dbt tests in stage 4.

2. **Extraction** (`ingestion/extract.py`) — pulls the `app.orders` rows
   touched on a given `run_date` from Postgres and stages the day's gateway /
   settlement CSVs alongside them (`data/<date>/staged_*.csv`).

3. **Validation & load** (`ingestion/validate.py`, `ingestion/load.py`) —
   validates each staged row against a data contract (required fields,
   non-negative numerics, parseable timestamps, allowed enum values).
   Rejects go to `raw.rejected`; valid rows are loaded idempotently into
   `raw.orders`, `raw.gateway_events`, `raw.settlements`.

4. **Transformation** (`dbt_upi/`) — staging views dedupe the bronze tables,
   a snapshot tracks merchant fee-rate history (SCD Type 2), and
   `fact_reconciliation` joins all three systems per order and flags every
   discrepancy class the generator injects (missing/late settlement, amount
   mismatch, duplicate charge, orphan payment, status mismatch). Custom dbt
   tests in `dbt_upi/tests/` compare the mart's output against the seeded
   answer key.

5. **Orchestration** (`airflow/dags/upi_reconciliation_dag.py`) — runs stages
   2-4 once per day, so a backfill of the DAG reprocesses one day per run.

## Error injection reference

See `generator/config.yaml` -> `injected_error_rates` for the exact rates,
and `dbt_upi/models/marts/fact_reconciliation.sql` for how each one is
detected downstream.
