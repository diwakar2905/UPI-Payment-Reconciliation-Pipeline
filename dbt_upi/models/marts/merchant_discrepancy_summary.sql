-- One row per merchant: which merchants are driving reconciliation
-- discrepancies, for a dashboard's merchant leaderboard / drill-down view.

with per_merchant as (
    select
        merchant_id,
        count(*) as total_orders,
        count(*) filter (where reconciliation_status = 'discrepancy') as discrepancy_count,
        count(*) filter (where is_missing_settlement) as missing_settlement_count,
        count(*) filter (where is_late_settlement) as late_settlement_count,
        count(*) filter (where is_amount_mismatch) as amount_mismatch_count,
        count(*) filter (where is_duplicate_charge) as duplicate_charge_count,
        count(*) filter (where is_status_mismatch) as status_mismatch_count,
        sum(order_amount) filter (where reconciliation_status = 'discrepancy') as discrepancy_amount
    from {{ ref('fact_reconciliation') }}
    where merchant_id is not null
    group by 1
)

select
    m.merchant_id,
    m.name,
    m.category,
    m.fee_rate,
    p.total_orders,
    p.discrepancy_count,
    round(100.0 * p.discrepancy_count / nullif(p.total_orders, 0), 2) as discrepancy_rate_pct,
    p.missing_settlement_count,
    p.late_settlement_count,
    p.amount_mismatch_count,
    p.duplicate_charge_count,
    p.status_mismatch_count,
    p.discrepancy_amount
from per_merchant p
join {{ ref('dim_merchants') }} m on m.merchant_id = p.merchant_id and m.is_current
