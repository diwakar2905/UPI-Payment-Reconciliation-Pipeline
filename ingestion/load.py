#!/usr/bin/env python3
"""Idempotent raw loading module.

Loads the valid_*.csv files produced by validate.py into the raw.* bronze
tables. Idempotent per run_date: existing rows for that run_date are deleted
before the fresh insert, so re-running `make run DATE=...` never duplicates data.
"""
import argparse
import csv
import sys
from pathlib import Path

from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.common import log_pipeline_run, timed_step, valid_path  # noqa: E402
from ingestion.db import get_db_cursor  # noqa: E402

TABLES = {
    "orders": {
        "table": "raw.orders",
        "columns": ["order_id", "merchant_id", "customer_vpa", "amount", "status", "created_at", "updated_at"],
    },
    "gateway": {
        "table": "raw.gateway_events",
        "columns": ["txn_id", "order_id", "gateway_status", "amount", "event_time", "event_type"],
    },
    "settlement": {
        "table": "raw.settlements",
        "columns": ["settlement_id", "txn_id", "gross_amount", "mdr_fee", "gst_on_fee", "net_settled", "settled_date"],
    },
}


def load_source(run_date, name):
    spec = TABLES[name]
    src = valid_path(run_date, name)
    if not src.exists():
        return 0

    with open(src, newline="") as f:
        reader = csv.DictReader(f)
        rows = [tuple(row[col] for col in spec["columns"]) for row in reader]

    with get_db_cursor(commit=True) as cur:
        cur.execute(f"DELETE FROM {spec['table']} WHERE run_date = %s", (run_date,))
        if rows:
            columns_sql = ", ".join(spec["columns"] + ["run_date"])
            values = [row + (run_date,) for row in rows]
            execute_values(cur, f"INSERT INTO {spec['table']} ({columns_sql}) VALUES %s", values)

    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Load validated data into raw.* tables for a run date")
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    run_date = args.run_date

    with timed_step() as timing:
        counts = {name: load_source(run_date, name) for name in TABLES}

    total = sum(counts.values())
    log_pipeline_run(run_date, "load", rows_in=total, rows_out=total,
                      duration_seconds=timing["duration_seconds"])

    for name, count in counts.items():
        print(f"[load] {run_date}: {name} loaded={count}")


if __name__ == "__main__":
    main()
