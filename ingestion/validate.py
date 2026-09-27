#!/usr/bin/env python3
"""Schema & data contract validator.

Reads the staged CSVs produced by extract.py, checks each row against a
simple data contract (required fields, non-negative numeric fields, parseable
timestamps), and splits rows into valid_*.csv (passed to load.py) and
raw.rejected (dead-letter table, with the offending row and a reason).
"""
import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.common import log_pipeline_run, staged_path, timed_step, valid_path  # noqa: E402
from ingestion.db import get_db_cursor  # noqa: E402

CONTRACTS = {
    "orders": {
        "required": ["order_id", "merchant_id", "customer_vpa", "amount", "status", "created_at", "updated_at"],
        "numeric": ["amount"],
        "timestamps": ["created_at", "updated_at"],
        "allowed_values": {"status": {"created", "paid", "failed", "refunded"}},
    },
    "gateway": {
        "required": ["txn_id", "order_id", "gateway_status", "amount", "event_time", "event_type"],
        "numeric": ["amount"],
        "timestamps": ["event_time"],
        "allowed_values": {"gateway_status": {"success", "failed"}},
    },
    "settlement": {
        "required": ["settlement_id", "txn_id", "gross_amount", "mdr_fee", "gst_on_fee", "net_settled", "settled_date"],
        "numeric": ["gross_amount", "mdr_fee", "gst_on_fee", "net_settled"],
        "timestamps": ["settled_date"],
        "allowed_values": {},
    },
}


def _parse_timestamp(value):
    return dt.datetime.fromisoformat(value)


def validate_row(row, contract):
    for field in contract["required"]:
        if not row.get(field):
            return f"missing required field: {field}"

    for field in contract["numeric"]:
        try:
            if float(row[field]) < 0:
                return f"negative value for {field}: {row[field]}"
        except (TypeError, ValueError):
            return f"non-numeric value for {field}: {row[field]!r}"

    for field in contract["timestamps"]:
        try:
            _parse_timestamp(row[field])
        except (TypeError, ValueError):
            return f"unparseable timestamp for {field}: {row[field]!r}"

    for field, allowed in contract["allowed_values"].items():
        if row.get(field) not in allowed:
            return f"unexpected value for {field}: {row[field]!r}"

    return None


def validate_source(run_date, name):
    contract = CONTRACTS[name]
    src = staged_path(run_date, name)
    valid_rows, rejects = [], []
    fieldnames = contract["required"]

    if src.exists() and src.stat().st_size > 0:
        with open(src, newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or fieldnames
            for row in reader:
                reason = validate_row(row, contract)
                if reason:
                    rejects.append((row, reason))
                else:
                    valid_rows.append(row)

    out_path = valid_path(run_date, name)
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(valid_rows)

    if rejects:
        with get_db_cursor(commit=True) as cur:
            cur.executemany(
                """
                INSERT INTO raw.rejected (source, row_data, reason, run_date)
                VALUES (%s, %s, %s, %s)
                """,
                [(name, json.dumps(row), reason, run_date) for row, reason in rejects],
            )

    return len(valid_rows), len(rejects)


def main():
    parser = argparse.ArgumentParser(description="Validate staged data for a run date")
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    run_date = args.run_date

    with timed_step() as timing:
        totals = {name: validate_source(run_date, name) for name in CONTRACTS}

    rows_in = sum(v + r for v, r in totals.values())
    rows_out = sum(v for v, _ in totals.values())
    rows_rejected = sum(r for _, r in totals.values())
    log_pipeline_run(run_date, "validate", rows_in=rows_in, rows_out=rows_out,
                      rows_rejected=rows_rejected, duration_seconds=timing["duration_seconds"])

    for name, (valid, rejected) in totals.items():
        print(f"[validate] {run_date}: {name} valid={valid} rejected={rejected}")


if __name__ == "__main__":
    main()
