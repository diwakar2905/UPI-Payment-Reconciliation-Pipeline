# Interview prep — UPI Payment Reconciliation Pipeline

Every answer here is grounded in this repo's actual code — file names, column
names, and behavior are exact, not paraphrased. Read [README.md](README.md)
first for the full picture; this file is drilling, not narrative.

How to use this: read a section, close the file, explain it out loud in your
own words. If you can't, that's the section to re-read. The "one-liners" at
the very end are for a last five-minute skim before you walk in.

---

## Table of contents

1. [The 60-second pitch](#1-the-60-second-pitch)
2. [Architecture & system design](#2-architecture--system-design)
3. [The generator (Python)](#3-the-generator-python)
4. [Ingestion (extract / validate / load)](#4-ingestion-extract--validate--load)
5. [SQL & Postgres](#5-sql--postgres)
6. [dbt — general concepts](#6-dbt--general-concepts)
7. [dbt — this project's models](#7-dbt--this-projects-models)
8. [Data engineering concepts](#8-data-engineering-concepts)
9. [Airflow / orchestration](#9-airflow--orchestration)
10. [Testing & data quality](#10-testing--data-quality)
11. [Real bugs found in this project (behavioral questions)](#11-real-bugs-found-in-this-project-behavioral-questions)
12. [Trade-offs & "why not X" questions](#12-trade-offs--why-not-x-questions)
13. [Rapid-fire one-liners](#13-rapid-fire-one-liners)

---

## 1. The 60-second pitch

**Q: Walk me through this project in a minute.**
A: It's a batch data pipeline that reconciles UPI payments across three
systems that don't natively agree — a merchant app's order DB, a payment
gateway's event log, and a bank's settlement file. I built a seeded synthetic
data generator that produces all three, deliberately injecting 8 realistic
discrepancy classes (missing settlements, late settlements, amount mismatches,
duplicate charges, orphan payments, status mismatches, corrupted rows,
refunds). A Python ingestion layer extracts, validates against a data
contract, and idempotently loads that data into Postgres. dbt then builds a
star schema on top — staging views, an SCD2 snapshot for merchant fee history,
and a core incremental fact table that joins all three systems per order and
flags every discrepancy with a specific boolean. I measure the pipeline's own
accuracy — precision/recall per error type — against the generator's ground
truth, not just "it runs." Airflow orchestrates it daily, CI runs the whole
thing end-to-end on every push, and there's a dashboard on top.

**Q: What's the single most important design decision in this project?**
A: Treating T+1 settlement lag and refunds as the *normal* case, not
exceptions. That one decision is why `fact_reconciliation` has to be
incremental in a non-obvious way (see section 7) and why the accuracy tests
need a maturity buffer (section 10) — almost every other design choice in the
project traces back to modeling real settlement timing honestly instead of
pretending everything settles instantly.

**Q: What would you do differently / what's not done?**
A: No demo video or hosted deployment (deliberately out of scope — see
README). The dashboard screenshots are rendered from a small custom HTML page
rather than a live Metabase instance, because Metabase wasn't reachable in
the environment I built this in (no Docker daemon, its jar isn't distributed
through any allowed package registry) — the `docker-compose.yml` Metabase
service is real and works if you have Docker locally, I just couldn't
exercise it myself. Given more time I'd add a `fact_payments`-level
duplicate-charge dashboard and tighten the day-over-day anomaly test from a
flat 50% threshold to something seasonality-aware.

---

## 2. Architecture & system design

**Q: Why three separate "systems" in the generator instead of one clean input?**
A: Because that's the actual shape of the real problem. A merchant's order
DB, a payment gateway, and a bank settlement file are three genuinely
independent systems in production, each with different latency, different
failure modes, and no shared transaction. Modeling them as three separate,
occasionally-disagreeing sources — rather than one clean joined table — is
what makes the reconciliation logic meaningful instead of trivial.

**Q: Why four Postgres schemas (`app`, `raw`, `staging`, `marts`)?**
A: Standard medallion-style layering, each with one job:
- `app` — the source-of-truth OLTP tables (stands in for the merchant's real
  database; the generator writes here directly, the pipeline only reads).
- `raw` — bronze. Exactly what was ingested for a given `run_date`, byte for
  byte, no transformation. This is what makes replays/debugging possible —
  you can always see exactly what arrived on a given day.
- `staging` — one deduped, standardized view per raw table, plus the SCD2
  merchant snapshot. No business logic yet, just cleanup.
- `marts` — the star schema and every business rule. Everything a consumer
  (dashboard, analyst, another service) should actually query.

Note: dbt's schema-naming convention means these show up in Postgres as
`staging_staging` and `staging_marts` (see README's Database schema section)
— the profile's target schema (`staging`) prefixes the custom `+schema:`
config. Worth knowing so you don't go looking for a bare `marts` schema.

**Q: Where would this break at 10x scale? 100x?**
A: `extract.py`'s `SELECT * FROM app.orders WHERE updated_at::date = %s` is
indexed (`idx_app_orders_updated_at`) and stays a single partition scan
regardless of overall table size, so that scales fine. The real pressure
points: (1) `load.py`'s `DELETE FROM raw.<table> WHERE run_date = %s` followed
by a bulk insert becomes a bigger transaction as daily volume grows — at real
scale you'd want partitioning by `run_date` so the delete is a partition drop,
not a row-by-row delete; (2) `fact_reconciliation`'s `delete+insert`
incremental strategy has the same shape — fine at thousands of affected
orders per run, would need a proper `merge` strategy or partition-swap
approach at millions; (3) the generator itself writes with `execute_values`
in one shot per day, which is fine for demo volumes but would want batching
at real production volumes.

**Q: How would you extend this to a fourth data source (say, a customer
support/dispute system)?**
A: Same pattern as everything else here: a `stg_disputes` staging view over a
new `raw.disputes` table, ingested through the same
extract→validate→load shape (a fourth `CONTRACTS`/`TABLES` entry in
`validate.py`/`load.py`), then join it into `fact_reconciliation` the same
way settlements are joined — by whatever key links a dispute back to a
`txn_id` or `order_id`, with its own `is_*` flag and its own slot in the
`reconciliation_status` CASE statement.

---

## 3. The generator (Python)

File: `generator/generate_data.py`. `Simulator` class holds all state
(merchants, pending orders, settlements-by-date, the answer key).

**Q: How is determinism guaranteed? Why does it matter?**
A: Everything that needs randomness draws from one seeded
`random.Random(seed)` instance (`self.rng`), and `Faker.seed(seed)` seeds the
fake-name generator too. Given the same seed and the same `--days`/
`--orders-per-day`, you get byte-identical output every run. It matters
because (a) the generator's own `answer_key.csv` is only useful as ground
truth if it's reproducible — otherwise you can't re-verify a bug fix against
the same data — and (b) it makes CI runs and local runs comparable.

**Q: You mentioned IDs used to be `uuid.uuid4()` — why was that a bug, and
what's the fix?**
A: `uuid.uuid4()` draws from `os.urandom()`, which is *not* seedable — you'd
get a different order_id/merchant_id/txn_id every run even with
`random.Random(42)` fixed everywhere else, quietly breaking the "seeded
simulator" promise. The fix: a small helper,
`self.rng.getrandbits(length * 4)` formatted as hex
(`_rand_hex(length)`), which draws from the same seeded RNG as everything
else. Same format (hex string), same call sites, fully deterministic.

**Q: Walk me through what happens for one order, start to finish, in the
generator.**
A: `_generate_order` picks a merchant, an amount, and rolls a weighted status
(`created`/`failed`/`refunded`/`paid`, with `pending_resolved` rate deciding
`created`). If it's `created`, the order goes on a `pending_orders` list with
a due date 1-2 days out and gets an early "pending" answer-key entry — it'll
be resolved on a later day by `_resolve_pending_order`. Otherwise it goes
straight into `_settle_order`: compute the *expected* gateway status
(`success` for paid/refunded, `failed` for failed), maybe flip it
(`status_mismatch`), write the primary gateway event (maybe corrupting it —
`invalid_row`), maybe add a duplicate event. If the gateway ended up
`failed`, we're done — record the answer and return. If `success`: maybe skip
the settlement entirely (`missing_settlement`), otherwise pick a settlement
date (T+1 normally, T+3..T+6 for `late_settlement`), maybe shift the amount
(`amount_mismatch`), compute fee/GST/net, maybe corrupt the row
(`invalid_row`), write it. If the order was `refunded`, write a *second*
settlement row (`settlement_type='refund'`, negated amounts, no fee reversed,
1-3 days after the payment settlement) and record `refunded` as the answer.

**Q: Why is refund handled as a second settlement row instead of, say,
mutating the original one or a separate table?**
A: A refund is a genuine second business event — the payment happened, then
later, separately, some of it came back. Mutating the original settlement
would destroy the audit trail (you'd lose the record that the original
charge actually settled). A whole separate table would need its own schema,
its own staging model, its own join logic for something that's really "the
same kind of thing, opposite sign." A `settlement_type` discriminator column
on the existing table keeps one schema, one staging model, and lets
`fact_reconciliation` pick both rows apart with a simple `where
settlement_type = 'payment'` / `'refund'` filter (see `settlement_per_txn`
and `refund_per_txn` CTEs).

**Q: Why does the "corrupt a date field" logic use a regex
(`^\d{4}-\d{2}-\d{2}`) instead of checking for a literal `"T"`?**
A: Because not every date-like field in this project is a full ISO timestamp.
`created_at`/`updated_at`/`event_time` are (`2026-09-01T00:00:00+00:00`,
contains `"T"`), but `settled_date` is a bare date (`2026-09-01`, no `"T"` at
all). The original version checked for `"T" in v`, which silently never
matched `settled_date` — so `invalid_row` corruption targeting a settlement
row's date would find nothing to corrupt and leave the row completely clean,
while the generator's own bookkeeping still recorded it as corrupted. Matching
on the date prefix pattern instead catches both shapes.

**Q: How does the generator avoid double-counting error types when multiple
error rolls hit the same order?**
A: `error_type = error_type or "X"` throughout `_settle_order` — first error
rolled wins, and it's a simple "if not already set" pattern, not a list. This
is a deliberate simplification: the ground truth answer key is single-label
per order (matches its `expected_result` column), while `fact_reconciliation`
computes independent boolean flags and can multi-label the same order. That
asymmetry is exactly why `accuracy_by_error_type`'s precision numbers aren't
all 1.0 — an order flagged for two real problems only "counts" for one in the
ground truth, so the second flag looks like a false positive even though it's
correct. That's a known, documented property of the scoring, not a bug.

**Q: What does `config.yaml` control, and why keep it separate from the code?**
A: Volumes (`days`, `orders_per_day`, `merchant_count`,
`merchants_updated_per_day`), the random seed, and every injected error rate,
plus output paths. Separating it from code means tuning noise levels (e.g.
"I want 10% duplicate charges instead of 1%, to stress-test the
`fact_payments` model") is a config edit, not a code change — and it keeps
all eight error rates visible in one place instead of scattered through the
simulator's logic.

---

## 4. Ingestion (extract / validate / load)

Files: `ingestion/extract.py`, `ingestion/validate.py`, `ingestion/load.py`,
plus shared helpers in `ingestion/common.py` and `ingestion/db.py`.

**Q: Why three separate scripts instead of one?**
A: Single responsibility, and it mirrors how you'd want to retry/monitor a
real pipeline: if validation logic changes, you don't need to touch the
extract or load code, and Airflow gets natural per-stage retry/observability
boundaries (each is its own `BashOperator` task).

**Q: What makes `extract.py` "incremental"?**
A: `SELECT ... FROM app.orders WHERE updated_at::date = %s` — it only ever
pulls rows touched on the exact `run_date` it's given, never the whole
table, backed by `idx_app_orders_updated_at`. Gateway/settlement CSVs are
already partitioned by day at the filesystem level
(`data/<run_date>/gateway.csv`), so "incremental" there is just "read this
one day's file."

**Q: What's the actual data contract in `validate.py`? Be specific.**
A: A `CONTRACTS` dict, one entry per source (`orders`, `gateway`,
`settlement`), each specifying: `required` fields (missing → reject),
`numeric` fields that must be non-negative (with one named exception —
`signed_if_refund = {"gross_amount", "net_settled"}`, only allowed negative
when `row["settlement_type"] == "refund"`), `timestamps` that must parse via
`datetime.fromisoformat`, and `allowed_values` for enum-like fields
(`status`, `gateway_status`, `settlement_type`). `validate_row` runs all four
checks in order and returns the first failure reason as a string, or `None`
if the row's clean.

**Q: Why does the negative-amount check have an exception for refunds
specifically, rather than just allowing negative amounts everywhere?**
A: Because a negative amount is a real, meaningful data-quality signal
*everywhere else* — a negative order amount or gateway amount should never
happen and is worth rejecting. Only a settlement row explicitly marked
`settlement_type='refund'` is allowed to be negative, and only for the two
fields that are supposed to be (`gross_amount`, `net_settled`) —
`mdr_fee`/`gst_on_fee` still have to be non-negative even on a refund row,
because the generator sets those to `0.0` (fees aren't reversed), never
negative.

**Q: How is `load.py` idempotent? Prove it.**
A: `DELETE FROM raw.<table> WHERE run_date = %s` runs immediately before the
bulk insert, inside the same `get_db_cursor(commit=True)` transaction. So
"load day X" is really "replace day X's data" — run it once, run it a
hundred times, `raw.*` ends up with exactly one copy of that day's valid
rows either way. This was verified directly, not just asserted: running
`make run DATE=...` twice in a row and diffing `raw.orders` row counts/content
before and after showed no duplication.

**Q: What happens to a rejected row? Where does it go, and what do you get
back?**
A: `raw.rejected(source, row_data, reason, run_date)` — `row_data` is the
*entire original row* as JSONB (so nothing is lost, you can always see
exactly what was rejected), `reason` is the specific string
`validate_row` returned (e.g. `"negative value for gross_amount: -50.00"`),
and it's tagged with `source` (`orders`/`gateway`/`settlement`) and
`run_date` so you can query "what got rejected on 2026-09-05, and why."

**Q: Where are row counts and timing recorded, and why?**
A: `raw.pipeline_runs(run_date, step, rows_in, rows_out, rows_rejected,
duration_seconds)`, written by `log_pipeline_run` in `ingestion/common.py`,
called at the end of `extract.py`, `validate.py`, and `load.py`. It's the
pipeline's own audit trail — the "Ingestion throughput" number in the
README's measured numbers comes directly from summing this table, and
several dbt models (the maturity-buffer logic in
`assert_reconciliation_matches_answer_key`, `accuracy_by_error_type`, and
`daily_reconciliation_summary`'s aging columns) use `max(run_date)` from this
same table as their "how far has this pipeline actually progressed" reference
point, instead of wall-clock time.

**Q: Why use `max(run_date)` from `pipeline_runs` instead of `now()`/
`current_date` for the maturity buffer?**
A: Because the pipeline's synthetic dates (`2026-09-01` etc.) have no
relationship to wall-clock "today," and in a real production system you'd
want the buffer to reflect *what this pipeline has actually ingested*, not
the literal calendar — a backfill catching up on old dates should be judged
against its own progress, not today's date. `max(run_date)` is the
pipeline's own notion of "now."

---

## 5. SQL & Postgres

**Q: Explain `distinct on` and why it's used in `fact_reconciliation`/
`fact_payments` instead of a window function + filter.**
A: `select distinct on (order_id) ... order by order_id, (gateway_status =
'success') desc, event_time asc` is Postgres-specific shorthand: for each
distinct value of the `distinct on` expression, keep only the first row after
sorting. It's used to pick the "primary" gateway event per order — preferring
a successful event, and the earliest one if there's a tie — in one compact
statement instead of a `row_number() over (...) qualify rn = 1` pattern (which
Postgres doesn't even support natively — you'd need a subquery + `where rn =
1`, more verbose for the same result).

**Q: Explain `count(*) filter (where ...)`. Why not `sum(case when ... then 1
else 0 end)`?**
A: Both compute the same thing — a conditional count — but `filter (where
...)` is the SQL-standard, more readable form, and it lets multiple
differently-filtered aggregates share one `group by` pass without a wall of
`case` expressions. Used throughout the summary marts, e.g.
`daily_reconciliation_summary`'s `count(*) filter (where reconciliation_status
= 'matched')`.

**Q: What indexes exist, and why those specifically?**
A: `idx_app_orders_updated_at` (what `extract.py`'s incremental pull filters
on), `idx_app_orders_merchant_id` / `idx_app_merchants_updated_at` (join and
snapshot-strategy support), and on every `raw.*` table: an index on
`run_date` (every ingestion step filters/deletes by it) plus one on the
table's natural key (`order_id`, `txn_id`, `settlement_id`) for the staging
views' dedup joins.

**Q: Why `NUMERIC(12,2)` for amounts instead of `FLOAT`/`DOUBLE PRECISION`?**
A: Money should never use binary floating point — `0.1 + 0.2` isn't exactly
`0.3` in IEEE 754, and that kind of rounding error compounding across
thousands of fee calculations (`mdr_fee = gross_amount * fee_rate`, `gst_on_fee
= mdr_fee * 0.18`) would eventually produce real, if tiny, reconciliation
mismatches that are entirely artifacts of the storage type, not real
discrepancies. `NUMERIC` is exact decimal arithmetic.

**Q: What does `ON CONFLICT (order_id) DO UPDATE SET ...` do in the
generator's `_write_orders`, and why not `ON CONFLICT DO NOTHING`?**
A: It's an upsert: if `order_id` already exists (e.g. a pending order being
resolved on a later day, or simply re-running the generator with the same
seed), update its `status`/`updated_at` rather than erroring or silently
skipping. `DO NOTHING` would mean a pending order's resolution (`created` →
`paid`/`failed`) never actually updates the row in `app.orders` — the whole
"resolve pending orders on a later day" mechanic depends on the upsert
actually updating.

---

## 6. dbt — general concepts

**Q: What's the difference between a dbt model, a source, a seed, and a
snapshot?**
A: A **model** is a `.sql` file dbt compiles and runs — a `SELECT` that
becomes a view or table (`stg_orders`, `fact_reconciliation`). A **source**
is a reference to a table dbt doesn't manage — here, the `raw.*` and `app.*`
tables the ingestion scripts populate directly (declared in
`_staging__sources.yml`, referenced via `{{ source('raw', 'orders') }}`). A
**seed** is a CSV dbt loads verbatim as a table — `answer_key.csv`. A
**snapshot** captures point-in-time state over time (SCD2) —
`merchants_snapshot`.

**Q: What's `ref()` vs `source()`, and why does it matter?**
A: `ref('stg_orders')` points to another dbt model — dbt resolves it,
tracks it in the DAG, and knows the build order. `source('raw', 'orders')`
points to a table dbt does *not* build — it's an entry point into the DAG,
documented so dbt can still test/document it (see the `not_null`/`unique`
tests on `_staging__sources.yml`'s `columns`), but there's no "run this to
build it" step. Getting this wrong (e.g. hardcoding `raw.orders` instead of
`{{ source(...) }}`) loses dbt's lineage graph and its ability to detect when
a source table changed.

**Q: What is a dbt snapshot, mechanically? What columns does it add?**
A: A snapshot is a *table dbt itself owns and appends to* — every time you
run `dbt snapshot`, it compares the current source query's rows to what's
already in the snapshot table by `unique_key`, and for any row whose tracked
columns changed (per the chosen `strategy`), it closes the old version
(`dbt_valid_to = now`) and inserts a new one (`dbt_valid_from = now`,
`dbt_valid_to = null`). Two strategies: `timestamp` (compares an
`updated_at`-style column — used here) or `check` (compares an explicit list
of columns). `merchants_snapshot` uses `strategy='timestamp',
updated_at='updated_at'`.

**Q: What's the difference between the `timestamp` and `check` snapshot
strategies, and why was `timestamp` chosen here?**
A: `timestamp` trusts a single "this row changed" column
(`updated_at`) — cheap, and correct as long as the source reliably bumps that
column on every real change. `check` compares a named list of columns
directly, with no trust assumption, at the cost of specifying (and
maintaining) that list. `app.merchants.updated_at` is already bumped by the
generator on every fee-rate change (`churn_merchants`), so `timestamp` is
both simpler and sufficient — there's no column drift risk to guard against
with `check`.

**Q: What are dbt tests, and what are the four built-in "generic" ones?**
A: Assertions that run as part of `dbt test`, each compiling to a query that
should return zero rows. The four generic ones, usable via YAML with no SQL:
`not_null`, `unique`, `accepted_values` (column must be in a fixed list — used
for `status`, `gateway_status`, `settlement_type`, `reconciliation_status`),
and `relationships` (referential integrity against another model/source —
used for `fact_reconciliation.merchant_id` against `dim_merchants`). Beyond
those, a **singular test** is just a `.sql` file in `tests/` that returns
failing rows directly — used here for
`assert_reconciliation_matches_answer_key`,
`assert_no_negative_payment_amounts`, `assert_no_daily_row_count_anomaly`, and
`assert_no_orphan_settlements`.

**Q: What does `severity: warn` do, and when did you choose it over the
default `error`?**
A: A `warn`-severity test failure is reported but doesn't fail the `dbt test`
command's exit code — useful for a signal you want visible but that
shouldn't block a pipeline run. Used on `assert_no_orphan_settlements` (a
non-zero count is *expected* noise from invalid-row corruption and
late-arriving settlements outside the ingested window, not a real defect) and
`assert_no_daily_row_count_anomaly` (a heuristic threshold, not a hard
correctness rule — real day-to-day volume swings shouldn't halt the pipeline).

**Q: What does `{{ config(...) }}` do at the top of a model?**
A: Sets model-level configuration — most importantly here,
`materialized` (`view`/`table`/`incremental`), `unique_key` (the incremental
merge key), and `incremental_strategy`. It's Jinja, evaluated at compile
time, so it can also reference `var(...)` and conditionals
(`{% if is_incremental() %}`).

---

## 7. dbt — this project's models

**Q: What is `fact_reconciliation`'s grain? Walk me through its CTEs.**
A: One row per order, plus one row per orphan gateway payment (a payment
event with no backing order). CTEs, in order: `orders`/`gateway`/`settlements`
(thin wrappers on the staging refs) → `gateway_per_order` (event count per
order, for duplicate detection) → `primary_gateway_event` (the one gateway
event that represents the order, via `distinct on`) →
`settlement_per_txn`/`refund_per_txn` (the payment settlement and, if any, its
refund, split by `settlement_type`) → `matched` (orders joined to their
gateway/settlement/refund) → `orphans` (gateway successes with no matching
order) → `unioned` (both, stacked) → (conditionally) `affected_order_ids` →
`flagged` (the five boolean `is_*` columns computed) → the final `select`
(the `reconciliation_status` CASE, filtered to `affected_order_ids` on an
incremental run).

**Q: List the five discrepancy flags and their exact logic.**
A:
- `is_duplicate_charge` — `event_count > 1` (more than one gateway event for
  the order, regardless of status).
- `is_status_mismatch` — `(order_status in ('paid','refunded') and
  (gateway_status is null or gateway_status = 'failed')) or (order_status =
  'failed' and gateway_status = 'success')`.
- `is_missing_settlement` — `order_status in ('paid','refunded') and
  gateway_status = 'success' and settlement_id is null`.
- `is_amount_mismatch` — `settlement_id is not null and gross_amount !=
  order_amount`.
- `is_late_settlement` — `settlement_id is not null and settled_date >
  gateway_event_time::date + 1` (T+1 itself is never late).
- (plus `is_refunded` — `refund_settlement_id is not null`, which feeds the
  `refunded` status rather than being a "discrepancy" flag.)

**Q: Walk me through the final `reconciliation_status` CASE, in order.**
A: `created` order status → `pending`. An orphan payment → `discrepancy`
outright. Any of the five discrepancy flags true → `discrepancy` (this branch
runs *before* the "matched"/"refunded" branches, so a flagged order can never
also be called clean). A refunded order with a successful gateway event, a
real settlement, and a real refund row → `refunded`. A paid order with
success + settlement → `matched`. A failed order with a failed gateway event
→ `matched` (failing cleanly, consistently, is the *correct* outcome for a
failed order — not a discrepancy). Anything else → `discrepancy` (the
catch-all; should be unreachable in practice given the branches above, but
protects against an unmodeled combination silently reading as `matched`).

**Q: Why is `fact_reconciliation` `materialized='incremental'` with
`incremental_strategy='delete+insert'`, specifically (not `merge`, not
`append`)?**
A: Postgres's dbt adapter doesn't support a native `merge` strategy the way
warehouses like Snowflake/BigQuery do; `delete+insert` is the standard
Postgres-adapter equivalent — delete any existing row matching the new
batch's `unique_key` values, then insert the new batch. `append` would be
wrong here because reprocessing an already-loaded date would create
duplicate rows per order — the opposite of idempotent.

**Q: The incremental filter isn't just "orders created today" — why, and
what does it actually filter on?**
A: Because settlement lags payment by T+1 and refunds lag settlement by a
further 1-3 days, an order from three days ago can have *new* information
(its settlement, or its refund) arrive today, and that order's fact row needs
to be recomputed — even though the order itself is old. So
`affected_order_ids` is a `union` of order IDs touched three different ways
today: new/updated rows in `stg_orders` with `run_date = today`, new rows in
`stg_gateway_events` with `run_date = today`, and — the non-obvious one — new
rows in `stg_settlements` with `run_date = today`, joined back to `gateway`
to recover the `order_id` (settlements don't carry `order_id` directly, only
`txn_id`). Anything not in that union is left completely alone by the
`delete+insert`.

**Q: How did you verify the incremental logic actually works, beyond just
believing the SQL?**
A: Ran a real cross-day test: built the table for days 1-8, recorded the
Postgres `xmin` system column (the row's physical version) for a handful of
day-1 rows, then ingested day 9 (which included some day-8 orders'
newly-arrived T+1 settlements) and re-ran `dbt run`. Confirmed two things
directly: the day-1 rows' `xmin` was unchanged (proof Postgres never
physically touched them), and specific day-8 orders whose settlement arrived
on day 9 correctly flipped from an unsettled state to `matched` in that same
run.

**Q: What's `fact_payments`, and how is it different from
`fact_reconciliation`?**
A: Transaction-grain instead of order-grain — one row per gateway event
(`txn_id`), including every duplicate as its own separate row (whereas
`fact_reconciliation` collapses duplicates down to one "primary" event per
order via `distinct on`). It's the other half of the star schema: use
`fact_reconciliation` to ask "is this order OK," use `fact_payments` to ask
"show me every individual gateway transaction," e.g. for a duplicate-charge
audit or a raw payments ledger.

**Q: What does `accuracy_by_error_type` compute, and why does it matter more
than a simple pass/fail test?**
A: Per error type (`missing_settlement`, `late_settlement`,
`amount_mismatch`, `duplicate_charge`, `status_mismatch`), true positives,
false positives, false negatives, precision, recall, and F1 — comparing each
`is_*` flag in `fact_reconciliation` against the generator's single-label
`answer_key.error_type`, restricted to orders old enough to have fully
settled (same 5-day maturity buffer as the singular test). It matters because
a binary pass/fail test only tells you "something's wrong somewhere"; a
precision/recall breakdown tells you *which detector* is unreliable and in
which direction (e.g. `status_mismatch` sitting at 0.59 precision here — real,
not a bug — is a direct, measured consequence of the single-label vs.
multi-flag asymmetry described in section 3, not a flaw in the detection
logic itself).

**Q: Why exclude orphan payments from `accuracy_by_error_type`?**
A: They have no `app.orders` row by construction — the generator never adds
them to `self.answer_key` (which is keyed by real `order_id`s), so there's no
ground truth to score them against. They're covered by a different check
instead (`assert_no_orphan_settlements`, which looks at whether a settlement
references a `txn_id` with no backing gateway event — a related but distinct
data-integrity question).

**Q: What's `daily_reconciliation_summary`'s `unsettled_amount`, and how is
"aging" computed without depending on wall-clock time?**
A: `unsettled_amount` sums `order_amount` for orders that should have settled
(paid/refunded, gateway success) but have no `settlement_id` yet — real
money in limbo, whether that's a genuine gap or just not-yet-arrived data.
Aging (`avg_discrepancy_age_days`/`max_discrepancy_age_days`) is `(the
pipeline's own "as of" date) - order_created_at`, where "as of" is
`max(run_date)` from `raw.pipeline_runs` (a `pipeline_progress` CTE), not
`current_date` — so it means the same thing whether you're looking at a
live pipeline or a historical backfill.

---

## 8. Data engineering concepts

**Q: Define idempotency. Give this project's concrete example.**
A: An idempotent operation produces the same result no matter how many times
you apply it. Here: `load.py`'s delete-then-insert per `run_date`, and
`fact_reconciliation`'s incremental delete+insert keyed on `order_id` — both
mean re-running the exact same command twice never duplicates data. The
opposite (a naive `INSERT` with no delete/upsert) would double every row on a
second run — exactly the kind of bug that's invisible in a demo and
catastrophic in production once a retry happens.

**Q: What's an incremental model, and what problem does it solve that a
full-refresh table doesn't?**
A: A model that only recomputes the rows affected by *new* data, instead of
rebuilding the entire table from scratch every run. At real volumes, a
full-refresh `fact_reconciliation` over months of orders would mean every
single daily run re-scans and rebuilds the entire history — increasingly slow
and wasteful as the table grows, when in reality only a small, definable
slice of rows (this run's new/late-arriving data) actually changed.

**Q: What's "late-arriving data," and how is it different from a normal
late/delayed load?**
A: Late-arriving data is information that legitimately belongs to an
*already-processed* entity, arriving after that entity was first loaded — not
a failure, an expected characteristic of the domain. Here: an order
processed (and marked "unsettled") on day 3 has its settlement legitimately
arrive on day 4, and its refund on day 6-7. The system has to be *designed*
to revisit day-3's fact row when day-4's or day-6's data lands — that's the
entire reason `fact_reconciliation`'s incremental filter looks at
settlement/gateway `run_date`, not just order `run_date`.

**Q: What's a maturity/watermark buffer, and why does this project need
one?**
A: A rule that says "don't judge this row as final until enough time has
passed for all its expected late-arriving data to have shown up." Here: a
5-day buffer (T+1 settlement + up to 3 days refund lag + 1 day slack),
applied in `assert_reconciliation_matches_answer_key` and
`accuracy_by_error_type`, so the newest few days of any run — which are
*correctly* still showing as unsettled — don't get scored as if they were
final and wrong.

**Q: Explain SCD Type 2 in your own words, and why it's not just "add an
updated_at column."**
A: Slowly Changing Dimension Type 2 keeps *every* historical version of a
row, each with a validity window (`dbt_valid_from`/`dbt_valid_to`), instead of
overwriting in place. A plain `updated_at` column only tells you the *current*
value and when it last changed — it can't answer "what was this merchant's
fee rate on the day this specific order was placed," because the old value
is gone. SCD2 keeps that old value around, closed off with a validity range,
specifically so historical joins (like `fact_reconciliation`'s MDR
calculation) use the rate that was actually in effect at the time.

**Q: What's a dead-letter queue/table, and where is it here?**
A: A place bad/rejected records go instead of being silently dropped or
crashing the pipeline — `raw.rejected` here, storing the full original row
(as JSONB) plus the specific reason it failed validation. It means a data
quality problem is *visible and queryable*, not lost.

**Q: What's the medallion architecture (bronze/silver/gold), and how does
this project map onto it?**
A: Bronze = raw, unmodified ingested data (`raw.*` here). Silver = cleaned,
deduped, conformed data (`staging.*`/`stg_*` views here). Gold = business-level,
consumption-ready models (`marts.*` — the star schema — here). The point is
each layer has one job, so a bug in business logic (marts) never requires
re-ingesting data, and a bug in cleaning logic (staging) never requires
touching what was actually ingested (raw).

---

## 9. Airflow / orchestration

**Q: Describe the DAG's task graph and scheduling.**
A: `upi_reconciliation_dag.py` — six `BashOperator` tasks in a strict linear
chain: `extract >> validate >> load >> dbt_snapshot >> dbt_run >> dbt_test`.
`schedule="@daily"`, `start_date=2026-09-01`, `catchup=True`, `retries=3`,
`retry_delay=5 minutes`, `max_active_runs=1`. Each task's `bash_command`
passes Airflow's `{{ ds }}` (the logical date) as `--run-date`/`--vars`,
mirroring `make run DATE=...` almost line for line.

**Q: What does `catchup=True` do, and why is it wanted here?**
A: When a DAG is turned on (or was paused and resumed), `catchup=True` means
Airflow schedules a run for *every* interval between `start_date` and now,
not just the next one going forward — i.e., a backfill happens automatically
by the scheduler, one DAG run per missed day, instead of needing a separate
manual backfill mechanism. That's exactly the semantics `make backfill`
replicates manually via a shell loop.

**Q: Why `max_active_runs=1`?**
A: Prevents two days' DAG runs from executing concurrently. Given
`fact_reconciliation`'s incremental logic keys off `run_date`-scoped
row-level deletes/inserts, two runs racing against the same tables
concurrently could step on each other; serializing runs avoids that class of
race condition entirely.

**Q: How does a failure propagate? Does a bad `dbt_test` actually stop
anything?**
A: `BashOperator` raises `AirflowException` on any non-zero exit code, and
Airflow's default `trigger_rule` (`all_success`) means a failed task blocks
every downstream task from running. `dbt test` itself exits non-zero when any
`error`-severity test fails (not `warn`-severity ones), so a real correctness
regression stops the DAG at that point — nothing downstream can silently
report success on top of it. Since `dbt_test` is the last task here, "stopped
downstream" mostly matters for earlier failures (e.g. a bad `load` blocking
`dbt_snapshot`/`dbt_run`/`dbt_test` from ever running against incomplete
data).

**Q: Why 3 retries with a 5-minute delay, not more or fewer?**
A: Enough to absorb a transient failure (a momentary DB connection blip, a
brief resource contention) without masking a real, persistent bug behind
endless retries — if it's still failing after 3 attempts 5 minutes apart,
that's a signal worth a human looking at, not something to keep silently
retrying.

---

## 10. Testing & data quality

**Q: What's the full test pyramid here, top to bottom?**
A: pytest (Python-layer unit tests: generator determinism, T+1 default,
refund reversal, every branch of the validator's data contract) → dbt schema
tests (`not_null`/`unique`/`accepted_values`/`relationships`, declarative,
one per model column) → dbt singular tests (custom SQL assertions:
answer-key matching, no negative amounts, day-over-day anomaly, orphan
settlements) → `accuracy_by_error_type` (a queryable accuracy *report*, not a
pass/fail gate, for measuring detector quality over time) → CI (runs the
entire path end-to-end against a real Postgres service container on every
push).

**Q: Give an example of a test you wrote specifically because a bug slipped
past the "obvious" tests.**
A: `accuracy_by_error_type` itself. `not_null`/`unique`/`accepted_values`
tests all passed even when `fact_reconciliation`'s CASE statement was
computing the five `is_*` flags but not actually referencing most of them —
the table had valid-looking data in every column, just *wrong conclusions*.
Only a test that checks computed output against independently-known-correct
ground truth (the seeded answer key) can catch "the logic is confidently
wrong," as opposed to "the data is malformed."

**Q: What's the difference between a schema test and a singular test, and
when do you reach for each?**
A: A schema test is declarative YAML for one of the four generic checks
(`not_null` etc.) — reach for it whenever the check is "this column, this
condition," nothing more. A singular test is a full SQL query you write by
hand, returning the rows that violate some rule — reach for it whenever the
check spans multiple tables, needs a join, needs a computed threshold, or
just doesn't fit the generic four (e.g. `assert_no_daily_row_count_anomaly`'s
day-over-day `lag()` comparison, or `assert_reconciliation_matches_answer_key`'s
join against the seed with a maturity-buffer filter).

**Q: Why is `assert_no_daily_row_count_anomaly` a `warn`, not an `error`?**
A: It's a statistical heuristic (>50% swing vs. the prior day), not a
guaranteed-correct rule — a legitimate business event (a marketing campaign,
a holiday, a known low-volume day) could trip it without anything actually
being broken. `warn` keeps it visible (worth a human glance) without letting
a heuristic block the pipeline the way a real correctness failure should.

**Q: What does CI actually run, and why does that matter more than "tests
pass locally"?**
A: The identical documented path a new developer would run by hand:
spin up Postgres, apply `sql/init.sql`, generate a small dataset, run
`extract`/`validate`/`load` for each day, then `dbt seed`/`snapshot`/`run`/
`test`, plus `pytest`. Running the *actual* commands end-to-end, against a
*real* (if ephemeral) Postgres — not a mocked DB, not a curated subset of
steps — is what catches the class of bug that only shows up in integration
(e.g. the `make run` target missing a `dbt seed` step, or `make init-db`'s
`PGPASSWORD` mismatch — both found by literally running the documented path
from a dropped database, not by unit testing individual pieces).

---

## 11. Real bugs found in this project (behavioral questions)

Interviewers love "tell me about a bug you found and how you fixed it." These
are all real, from this project, with root cause and fix — good material for
that question.

**Bug: non-deterministic IDs despite a "seeded simulator."**
*Symptom:* a test asserting "same seed → same output" failed on merchant IDs.
*Root cause:* IDs were generated with `uuid.uuid4()`, which draws from
`os.urandom()` and ignores `random.seed()`/`Faker.seed()` entirely.
*Fix:* a small `_rand_hex()` helper drawing from the project's own seeded
`random.Random` instance. *Lesson:* "I used a seeded RNG" isn't the same
claim as "every source of randomness in this code path is seeded" — you have
to check every call site that generates something random, including ones
that don't look like classic randomness (UUIDs).

**Bug: date corruption silently no-op'ing on settlement rows.**
*Symptom:* rows the generator believed it had corrupted (recorded as
`invalid_row` in the answer key) were passing validation cleanly.
*Root cause:* the corruption helper matched date-like fields by checking for
a literal `"T"` substring — true for full timestamps
(`2026-09-01T00:00:00+00:00`) but false for `settled_date`, a bare date
(`2026-09-01`) with no `"T"` anywhere. *Fix:* match on a date-prefix regex
(`^\d{4}-\d{2}-\d{2}`) instead, which catches both shapes. *Lesson:* an
assumption baked into a helper function ("dates look like X") can be true for
most call sites and silently false for one, and it'll only surface as a
downstream data-quality mismatch, not an error.

**Bug: `fact_reconciliation` computing flags it never used.**
*Symptom:* orders with real, detectable discrepancies (e.g. a duplicate
charge) were coming out `reconciliation_status = 'matched'`.
*Root cause:* the model computed five `is_*` boolean columns correctly, but
the final `CASE` statement's `matched` branch only checked
`order_status`/`gateway_status`/`settlement_id` directly — it never actually
referenced `is_duplicate_charge`, `is_status_mismatch`, etc. The columns
existed, were correctly computed, and were simply not read by the logic that
mattered. *Fix:* added an explicit branch checking all five flags *before*
the `matched`/`refunded` branches, so any real flag forces `discrepancy`
regardless of what the surface-level status columns look like. *Lesson:*
"the column has the right value" and "the code that decides the outcome
actually uses that column" are two different things to verify — this class
of bug is invisible to schema tests and requires checking computed output
against ground truth.

**Bug: refund + status_mismatch double-counted into the wrong bucket.**
*Symptom:* a small number of orders — refunded, with an injected
`status_mismatch` so the gateway showed `failed` instead of the expected
`success` — were coming out `matched` instead of `discrepancy`.
*Root cause:* the CASE statement's "failed order, failed gateway → matched"
branch was written as `order_status in ('failed', 'refunded') and
gateway_status = 'failed'` — but a refund is only ever valid *after* a
successful payment; a refunded order with a failed gateway event is a real
contradiction, not a clean "both sides say failed" case like a genuinely
failed order. *Fix:* narrowed that branch to `order_status = 'failed'` only,
and separately broadened `is_status_mismatch`'s definition to treat
`'refunded'` the same as `'paid'` (both expect gateway `success`). *Lesson:*
copy-pasting "these two statuses behave the same" across two different
branches of the same conditional is exactly the kind of subtle inconsistency
that only a full accuracy check (not a schema test) catches.

**Bug: `make run`/`make backfill` missing a required setup step.**
*Symptom:* running the fully documented path from a freshly dropped database
(`make init-db && make generate && make backfill`) failed with `relation
"staging.answer_key" does not exist`.
*Root cause:* `dbt run`/`dbt test` reference the `answer_key` seed (via
`accuracy_by_error_type` and the singular test), but the `run` Makefile
target only ever called `dbt snapshot && dbt run && dbt test` — never
`dbt seed`. CI happened to seed separately, so it never caught this; only
running the *documented single-command path* did. *Fix:* added `dbt seed`
to the front of the `run` target's chain. *Lesson:* a step that "someone
else already ran manually" during development can hide a genuine gap in the
automated path indefinitely — the real test is a stranger running the
README's instructions on a machine that has never seen this project before.

**Bug: `make init-db` silently prompting for a password.**
*Symptom:* `make init-db` on a fresh clone hung waiting for a password
instead of using the one in `.env`.
*Root cause:* `psql` reads credentials from the environment variable
`PGPASSWORD` specifically — the project's own convention is `PG_PASSWORD`
(matching `PG_HOST`/`PG_PORT`/etc.), and nothing translated between the two
names before invoking `psql` directly. *Fix:* `init-db`'s recipe now sets
`PGPASSWORD="$(PG_PASSWORD)"` explicitly for that one command. *Lesson:*
naming consistency *within* your own project doesn't guarantee compatibility
with a tool's own environment-variable conventions — check the actual tool's
expectations, not just your own naming scheme.

---

## 12. Trade-offs & "why not X" questions

**Q: Why Airflow and not a simpler scheduler (cron, a cloud-native scheduled
function)?**
A: Airflow gives retries, `catchup`/backfill semantics, task-level
observability, and a DAG structure that mirrors dependency order explicitly
— all things you'd otherwise hand-roll in a cron script. For a project this
size, cron would "work," but Airflow is the honest choice for what a real
production reconciliation pipeline would actually use, and it costs nothing
extra here since the DAG is a thin wrapper around the same
Makefile-equivalent commands.

**Q: Why dbt instead of writing the transformation logic directly in Python
(pandas) or as raw SQL scripts run by a custom runner?**
A: dbt gives testing, documentation, lineage, incremental materialization,
and snapshotting *for free*, as first-class citizens of the same tool that
runs the transformations — writing that infrastructure by hand in a custom
Python/SQL runner would mean rebuilding a worse version of dbt. SQL-first
also keeps the transformation logic close to the data (and the query
optimizer), rather than pulling everything into Python memory the way a
pandas-based approach would.

**Q: Why Postgres instead of a cloud warehouse (Snowflake/BigQuery/Redshift)?**
A: Free, runs anywhere (a laptop, CI, a Docker container), and this
project's data volumes (a batch pipeline processing thousands to tens of
thousands of rows/day) don't need warehouse-scale columnar storage or
massive parallel query engines — Postgres with proper indexing handles it
comfortably. The trade-off is real at bigger scale: no native `merge`
incremental strategy (hence `delete+insert`), no automatic clustering/
partitioning without doing it yourself.

**Q: Why generate synthetic data instead of using a real (anonymized)
dataset?**
A: Ground truth. With a real dataset you can eyeball whether reconciliation
*looks* right, but you can't know the actual correct answer for every single
row — with a seeded generator that records its own answer key as it injects
each discrepancy, you get exact, provable precision/recall numbers instead
of "it seems to work." It also sidesteps any real PII/payment-data handling
entirely.

**Q: Why a 5-day maturity buffer specifically, not 3 or 7?**
A: It's the sum of the two real lags plus slack: T+1 settlement, plus up to
3 days of refund lag on top of that (settlement date + 1-3 days), plus one
day of margin — so a genuinely clean order has had time for every piece of
its expected data to arrive before it's judged. Shorter would misclassify
still-settling orders as failures; longer would just delay catching real
problems for no benefit, since the generator's own lags top out at T+1+3.

**Q: Why not just make the dashboard screenshots with a real Metabase
instance?**
A: Tried, in the environment this was built in — no Docker daemon available,
and Metabase's distributable jar isn't published through
`downloads.metabase.com` (blocked by the sandbox's egress policy) or as a
GitHub release asset for any version (checked directly via `git ls-remote
--tags` and probing several version numbers). The `docker-compose.yml`
Metabase service is real and will work anywhere Docker is available; the
shipped screenshots are a working substitute built from the exact same live
query results, not a mockup.

---

## 13. Rapid-fire one-liners

Skim this list once, five minutes before you walk in.

- **Grain of `fact_reconciliation`**: one row per order (+ orphan payments).
- **Grain of `fact_payments`**: one row per gateway transaction (dupes included).
- **Idempotency mechanism**: delete-then-insert, keyed by `run_date` (raw) / `order_id` (marts).
- **Incremental strategy used**: `delete+insert` (Postgres has no native `merge`).
- **Incremental filter**: order/gateway/settlement row loaded with today's `run_date` — not "order created today."
- **SCD2 table**: `merchants_snapshot`, `strategy='timestamp'` on `updated_at`.
- **Normal settlement lag**: T+1. **Late settlement**: T+3..T+6.
- **Refund modeling**: second settlement row, `settlement_type='refund'`, negative amounts, no fee reversal.
- **Maturity buffer**: 5 days, measured from `max(run_date)` in `raw.pipeline_runs`, not wall-clock time.
- **Dead-letter table**: `raw.rejected` (source, full row as JSONB, reason, run_date).
- **5 discrepancy flags**: duplicate charge, status mismatch, missing settlement, amount mismatch, late settlement.
- **4 reconciliation statuses**: matched, discrepancy, refunded, pending.
- **Accuracy measured against**: the generator's own seeded `answer_key.csv`.
- **DAG chain**: extract → validate → load → dbt_snapshot → dbt_run → dbt_test.
- **DAG config**: `@daily`, `catchup=True`, `retries=3`, `max_active_runs=1`.
- **Why IDs aren't `uuid.uuid4()`**: not seedable, breaks determinism.
- **Postgres schema naming gotcha**: `+schema: marts` → actual schema `staging_marts`.
- **Money type**: `NUMERIC(12,2)`, never float.
- **CI runs**: the full documented path, against a real ephemeral Postgres service.
- **Biggest real bug found**: computed discrepancy flags that the final status logic never referenced.
