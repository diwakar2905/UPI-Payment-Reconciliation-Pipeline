#!/usr/bin/env python3
"""Seeded multi-system synthetic transaction simulator.

Simulates three independent systems that a real UPI merchant stack would
produce, and deliberately injects the discrepancy classes the pipeline is
built to catch:

  - app.orders / app.merchants  -> written directly into Postgres (source OLTP)
  - gateway events              -> data/<run_date>/gateway.csv   (daily raw)
  - settlement records          -> data/<run_date>/settlement.csv (daily raw)
  - ground truth                -> dbt_upi/seeds/answer_key.csv

Error classes (rates configured in generator/config.yaml):
  missing_settlement, late_settlement, amount_mismatch, duplicate_charge,
  orphan_payment, status_mismatch, invalid_row, pending_resolved
"""
import argparse
import csv
import datetime as dt
import random
import re
import sys
from pathlib import Path

import numpy as np
import yaml
from faker import Faker
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.db import get_connection  # noqa: E402

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "fashion", "food_delivery",
    "travel", "utilities", "entertainment", "healthcare",
]
GST_RATE = 0.18
DATE_LIKE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


class Merchant:
    __slots__ = ("merchant_id", "name", "category", "fee_rate", "created_at", "updated_at")

    def __init__(self, merchant_id, name, category, fee_rate, created_at):
        self.merchant_id = merchant_id
        self.name = name
        self.category = category
        self.fee_rate = fee_rate
        self.created_at = created_at
        self.updated_at = created_at


class Simulator:
    def __init__(self, cfg, fake, rng, np_rng):
        self.cfg = cfg
        self.fake = fake
        self.rng = rng
        self.np_rng = np_rng
        self.errors = cfg["injected_error_rates"]
        self.merchants = []
        # orders still in 'created' state, resolved on a later day
        self.pending_orders = []
        # settlement rows keyed by the run_date they should appear under
        self.settlements_by_date = {}
        # order_id -> ground truth answer key row
        self.answer_key = {}

    def _rand_hex(self, length):
        """Deterministic hex id, drawn from the seeded rng (unlike uuid4, which is not seedable)."""
        return f"{self.rng.getrandbits(length * 4):0{length}x}"

    # ------------------------------------------------------------------ setup
    def create_merchants(self, n):
        now = dt.datetime.combine(self.cfg["simulation"]["start_date_obj"], dt.time(0, 0), tzinfo=dt.timezone.utc)
        for _ in range(n):
            m = Merchant(
                merchant_id=f"M{self._rand_hex(12)}",
                name=self.fake.company(),
                category=self.rng.choice(MERCHANT_CATEGORIES),
                fee_rate=round(self.rng.uniform(0.005, 0.030), 4),
                created_at=now,
            )
            self.merchants.append(m)
        return self.merchants

    def churn_merchants(self, run_date, n):
        now = dt.datetime.combine(run_date, dt.time(12, 0), tzinfo=dt.timezone.utc)
        for m in self.rng.sample(self.merchants, min(n, len(self.merchants))):
            m.fee_rate = round(self.rng.uniform(0.005, 0.030), 4)
            m.updated_at = now

    # -------------------------------------------------------------- one day
    def run_day(self, run_date, orders_per_day):
        day_start = dt.datetime.combine(run_date, dt.time(0, 0), tzinfo=dt.timezone.utc)
        orders_rows = []
        gateway_rows = []

        # 1. resolve orders that were left 'created' on a previous day
        still_pending = []
        for order in self.pending_orders:
            due_date, order_row = order
            if due_date <= run_date:
                self._resolve_pending_order(order_row, run_date, orders_rows, gateway_rows)
            else:
                still_pending.append(order)
        self.pending_orders = still_pending

        # 2. generate today's new orders
        for _ in range(orders_per_day):
            self._generate_order(run_date, day_start, orders_rows, gateway_rows)

        # 3. orphan payments: gateway success with no matching app.orders row
        n_orphans = int(orders_per_day * self.errors["orphan_payment"])
        for _ in range(n_orphans):
            row, _ = self._make_gateway_event(
                order_id=f"ORD-GHOST-{self._rand_hex(10)}",
                amount=round(self.rng.uniform(50, 3000), 2),
                status="success",
                event_time=day_start + dt.timedelta(seconds=self.rng.randint(0, 86399)),
            )
            gateway_rows.append(row)

        self._write_orders(orders_rows)
        self._write_csv(run_date, "gateway.csv",
                         ["txn_id", "order_id", "gateway_status", "amount", "event_time", "event_type"],
                         gateway_rows)

    def _generate_order(self, run_date, day_start, orders_rows, gateway_rows):
        merchant = self.rng.choice(self.merchants)
        order_id = f"ORD-{self._rand_hex(16)}"
        amount = round(self.rng.uniform(10, 5000), 2)
        created_at = day_start + dt.timedelta(seconds=self.rng.randint(0, 86399))

        roll = self.rng.random()
        if roll < self.errors["pending_resolved"]:
            status = "created"
        elif roll < self.errors["pending_resolved"] + 0.15:
            status = "failed"
        elif roll < self.errors["pending_resolved"] + 0.15 + 0.05:
            status = "refunded"
        else:
            status = "paid"

        order_row = {
            "order_id": order_id,
            "merchant_id": merchant.merchant_id,
            "customer_vpa": f"{self.fake.user_name()}@upi",
            "amount": amount,
            "status": status,
            "created_at": created_at,
            "updated_at": created_at,
        }

        if status == "created":
            due_date = run_date + dt.timedelta(days=self.rng.choice([1, 2]))
            self.pending_orders.append((due_date, order_row))
            orders_rows.append(order_row)
            self.answer_key[order_id] = [order_id, merchant.merchant_id, amount, "pending", "pending", "pending"]
            return

        orders_rows.append(order_row)
        self._settle_order(order_row, merchant, run_date, created_at, gateway_rows)

    def _resolve_pending_order(self, order_row, run_date, orders_rows, gateway_rows):
        resolved_at = dt.datetime.combine(run_date, dt.time(12, 0), tzinfo=dt.timezone.utc)
        order_row["status"] = "failed" if self.rng.random() < 0.2 else "paid"
        order_row["updated_at"] = resolved_at
        orders_rows.append(order_row)
        merchant = next(m for m in self.merchants if m.merchant_id == order_row["merchant_id"])
        self._settle_order(order_row, merchant, run_date, resolved_at, gateway_rows)

    def _settle_order(self, order_row, merchant, run_date, event_time, gateway_rows):
        order_id = order_row["order_id"]
        amount = order_row["amount"]
        order_status = order_row["status"]

        gateway_status = order_status if order_status in ("paid", "failed") else "failed"
        error_type = None
        if self.rng.random() < self.errors["status_mismatch"]:
            error_type = "status_mismatch"
            gateway_status = "failed" if gateway_status == "success" or gateway_status == "paid" else "success"
        gateway_status = "success" if gateway_status == "paid" else gateway_status

        txn_id = f"TXN-{self._rand_hex(16)}"
        primary_row, primary_corrupted = self._make_gateway_event(order_id, amount, gateway_status, event_time, txn_id)
        gateway_rows.append(primary_row)
        if primary_corrupted:
            # the primary confirmation itself got mangled and will be rejected downstream,
            # so the order can no longer reconcile cleanly regardless of what follows.
            error_type = error_type or "invalid_row"

        if self.rng.random() < self.errors["duplicate_charge"]:
            error_type = error_type or "duplicate_charge"
            dup_time = event_time + dt.timedelta(minutes=self.rng.randint(1, 5))
            dup_row, _ = self._make_gateway_event(
                order_id, amount, gateway_status, dup_time, f"TXN-{self._rand_hex(16)}")
            gateway_rows.append(dup_row)

        if order_status != "paid" or gateway_status != "success":
            self.answer_key[order_id] = self._answer_row(
                order_id, merchant.merchant_id, amount, order_status, error_type)
            return

        if self.rng.random() < self.errors["missing_settlement"]:
            self.answer_key[order_id] = self._answer_row(
                order_id, merchant.merchant_id, amount, order_status, "missing_settlement")
            return

        settled_date = run_date
        if self.rng.random() < self.errors["late_settlement"]:
            error_type = error_type or "late_settlement"
            settled_date = run_date + dt.timedelta(days=self.rng.randint(2, 5))

        gross_amount = amount
        if self.rng.random() < self.errors["amount_mismatch"]:
            error_type = error_type or "amount_mismatch"
            delta = round(self.rng.uniform(1, 500), 2)
            gross_amount = round(amount + self.rng.choice([-1, 1]) * delta, 2)

        mdr_fee = round(gross_amount * merchant.fee_rate, 2)
        gst_on_fee = round(mdr_fee * GST_RATE, 2)
        net_settled = round(gross_amount - mdr_fee - gst_on_fee, 2)

        settlement_row = [
            f"SET-{self._rand_hex(16)}", txn_id, gross_amount, mdr_fee,
            gst_on_fee, net_settled, settled_date.isoformat(),
        ]
        if self.rng.random() < self.errors["invalid_row"]:
            error_type = error_type or "invalid_row"
            settlement_row = self._corrupt_row(settlement_row)

        self.settlements_by_date.setdefault(settled_date, []).append(settlement_row)
        self.answer_key[order_id] = self._answer_row(
            order_id, merchant.merchant_id, amount, order_status, error_type or "clean")

    def _make_gateway_event(self, order_id, amount, status, event_time, txn_id=None):
        row = [txn_id or f"TXN-{self._rand_hex(16)}", order_id, status, amount,
               event_time.isoformat(), "payment"]
        corrupted = self.rng.random() < self.errors["invalid_row"]
        if corrupted:
            row = self._corrupt_row(row)
        return row, corrupted

    def _corrupt_row(self, row):
        row = list(row)
        choice = self.rng.choice(["null_key", "negative_amount", "bad_date"])
        if choice == "null_key":
            row[0] = ""
        elif choice == "negative_amount":
            for i, v in enumerate(row):
                if isinstance(v, (int, float)):
                    row[i] = -abs(v)
                    break
        else:
            for i, v in enumerate(row):
                if isinstance(v, str) and DATE_LIKE_RE.match(v):
                    row[i] = "not-a-date"
                    break
        return row

    def _answer_row(self, order_id, merchant_id, amount, order_status, error_type):
        return [order_id, merchant_id, amount, order_status, error_type or "none",
                "match" if error_type in (None, "clean") else "discrepancy"]

    # ------------------------------------------------------------------ I/O
    def _write_orders(self, rows):
        if not rows:
            return
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO app.orders (order_id, merchant_id, customer_vpa, amount, status, created_at, updated_at)
                    VALUES %s
                    ON CONFLICT (order_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at
                """, [(r["order_id"], r["merchant_id"], r["customer_vpa"], r["amount"],
                       r["status"], r["created_at"], r["updated_at"]) for r in rows])
            conn.commit()
        finally:
            conn.close()

    def write_merchants(self):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO app.merchants (merchant_id, name, category, fee_rate, created_at, updated_at)
                    VALUES %s
                    ON CONFLICT (merchant_id) DO UPDATE SET
                        fee_rate = EXCLUDED.fee_rate,
                        updated_at = EXCLUDED.updated_at
                """, [(m.merchant_id, m.name, m.category, m.fee_rate, m.created_at, m.updated_at)
                      for m in self.merchants])
            conn.commit()
        finally:
            conn.close()

    def _write_csv(self, run_date, filename, header, rows):
        if not rows:
            return
        out_dir = Path(self.cfg["paths"]["output_dir"]) / run_date.isoformat()
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / filename
        write_header = not path.exists()
        with open(path, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(header)
            writer.writerows(rows)

    def flush_settlements(self):
        header = ["settlement_id", "txn_id", "gross_amount", "mdr_fee", "gst_on_fee", "net_settled", "settled_date"]
        for settled_date, rows in self.settlements_by_date.items():
            self._write_csv(settled_date, "settlement.csv", header, rows)

    def write_answer_key(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["order_id", "merchant_id", "amount", "order_status", "error_type", "expected_result"])
            for row in self.answer_key.values():
                writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic UPI reconciliation data")
    parser.add_argument("--config", default=str(Path(__file__).parent / "config.yaml"))
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--orders-per-day", type=int, default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    sim_cfg = cfg["simulation"]
    days = args.days or sim_cfg["days"]
    orders_per_day = args.orders_per_day or sim_cfg["orders_per_day"]
    start_date = dt.date.fromisoformat(sim_cfg["start_date"])
    sim_cfg["start_date_obj"] = start_date

    seed = sim_cfg["random_seed"]
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)
    fake = Faker()
    Faker.seed(seed)

    sim = Simulator(cfg, fake, rng, np_rng)
    sim.create_merchants(sim_cfg["merchant_count"])
    sim.write_merchants()

    for day_offset in range(days):
        run_date = start_date + dt.timedelta(days=day_offset)
        if day_offset > 0:
            sim.churn_merchants(run_date, sim_cfg["merchants_updated_per_day"])
            sim.write_merchants()
        sim.run_day(run_date, orders_per_day)
        print(f"[generate_data] day {run_date.isoformat()} done "
              f"({orders_per_day} orders, {len(sim.pending_orders)} pending carried forward)")

    sim.flush_settlements()
    sim.write_answer_key(cfg["paths"]["answer_key"])
    print(f"[generate_data] wrote answer key with {len(sim.answer_key)} rows to {cfg['paths']['answer_key']}")


if __name__ == "__main__":
    main()
