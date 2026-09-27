# Dashboard setup (Metabase)

`docker-compose.yml` includes a `metabase` service alongside Postgres.
Screenshots aren't checked in (Metabase needs a browser + a running
container, which don't exist in CI), but the setup below is a few minutes
of clicking once the stack is up.

## Start it

```
make docker-up
# ...run make init-db / make generate / make run at least once so the
# marts below have data...
```

Metabase is then at http://localhost:3000. First run walks you through
creating an admin account, then **Add a database**:

- Database type: PostgreSQL
- Host: `postgres` (the docker-compose service name), Port: `5432`
- Database name / user / password: match your `.env`

## Suggested questions / cards

Point each of these at the mart it names — they're already shaped for a
dashboard, so most become a single Metabase "question" with no extra SQL:

| Chart | Source | Notes |
|---|---|---|
| Daily reconciliation trend | `marts.daily_reconciliation_summary` | Line chart of `matched_count` / `discrepancy_count` / `refunded_count` / `pending_count` over `report_date`. |
| Discrepancy rate | `marts.daily_reconciliation_summary` | `discrepancy_rate_pct` as a single-number trend or gauge. |
| Unsettled amount | `marts.daily_reconciliation_summary` | `unsettled_amount` as a trend - money that should have settled but has no settlement row yet. |
| Mismatch aging | `marts.daily_reconciliation_summary` | `avg_discrepancy_age_days` / `max_discrepancy_age_days` by `report_date` - a discrepancy that's still open many days later is a real problem, not just settlement lag. |
| Merchant leaderboard | `marts.merchant_discrepancy_summary` | Table sorted by `discrepancy_rate_pct` desc, or a bar chart of `discrepancy_count` by `name`. |
| Discrepancy breakdown | `marts.merchant_discrepancy_summary` | Stacked bar of `missing_settlement_count` / `late_settlement_count` / `amount_mismatch_count` / `duplicate_charge_count` / `status_mismatch_count` per merchant. |
| Reconciliation drill-down | `marts.fact_reconciliation` | Filterable table for investigating a specific order/merchant/date. |
| Payments ledger | `marts.fact_payments` | Transaction-grain view (every gateway event, incl. duplicates) for a raw payments audit. |

Combine the first four into a single dashboard (**+ New dashboard**), and
add the fifth as a linked detail view via a dashboard filter on
`merchant_id` or `report_date`.

## Pipeline coverage

`daily_reconciliation_summary` and `merchant_discrepancy_summary` are dbt
marts under `dbt_upi/models/marts/`, built and tested the same way as
`fact_reconciliation` (`make dbt-run`, `make dbt-test`) — see
`dbt_upi/models/marts/_marts__models.yml` for their column tests.
