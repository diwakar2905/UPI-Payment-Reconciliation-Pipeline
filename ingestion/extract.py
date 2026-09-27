#!/usr/bin/env python3
"""Incremental extract module.

Pulls the app.orders rows touched on --run-date directly from Postgres, and
stages the generator's daily gateway/settlement CSVs alongside them under a
common `staged_*` naming scheme that validate.py consumes next.
"""
import argparse
import csv
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.common import log_pipeline_run, raw_path, staged_path, timed_step  # noqa: E402
from ingestion.db import get_db_cursor  # noqa: E402

ORDER_COLUMNS = ["order_id", "merchant_id", "customer_vpa", "amount", "status", "created_at", "updated_at"]


def extract_orders(run_date):
    with get_db_cursor() as cur:
        cur.execute(
            f"""
            SELECT {", ".join(ORDER_COLUMNS)}
            FROM app.orders
            WHERE updated_at::date = %s
            ORDER BY order_id
            """,
            (run_date,),
        )
        rows = cur.fetchall()

    out_path = staged_path(run_date, "orders")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ORDER_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows), out_path


def stage_csv_source(run_date, name):
    """Copy a generator-produced daily CSV into the staged/ naming scheme, if present."""
    src = raw_path(run_date, name)
    dest = staged_path(run_date, name)
    if not src.exists():
        with open(dest, "w") as f:
            f.write("")
        return 0
    shutil.copyfile(src, dest)
    with open(dest) as f:
        row_count = max(sum(1 for _ in f) - 1, 0)  # minus header
    return row_count


def main():
    parser = argparse.ArgumentParser(description="Extract orders/gateway/settlement data for a run date")
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    run_date = args.run_date

    with timed_step() as timing:
        n_orders, orders_path = extract_orders(run_date)
        n_gateway = stage_csv_source(run_date, "gateway")
        n_settlement = stage_csv_source(run_date, "settlement")

    total_rows = n_orders + n_gateway + n_settlement
    log_pipeline_run(run_date, "extract", rows_in=total_rows, rows_out=total_rows,
                      duration_seconds=timing["duration_seconds"])
    print(f"[extract] {run_date}: orders={n_orders} gateway={n_gateway} settlement={n_settlement} "
          f"-> {orders_path.parent}")


if __name__ == "__main__":
    main()
