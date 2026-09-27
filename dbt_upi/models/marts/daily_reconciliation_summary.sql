-- One row per calendar day: reconciliation health at a glance, the
-- natural top-level chart for a BI dashboard's daily trend view.

select
    order_created_at::date as report_date,
    count(*) as total_orders,
    count(*) filter (where reconciliation_status = 'matched') as matched_count,
    count(*) filter (where reconciliation_status = 'discrepancy') as discrepancy_count,
    count(*) filter (where reconciliation_status = 'pending') as pending_count,
    round(
        100.0 * count(*) filter (where reconciliation_status = 'discrepancy')
            / nullif(count(*), 0),
        2
    ) as discrepancy_rate_pct,
    sum(order_amount) filter (where reconciliation_status = 'discrepancy') as discrepancy_amount,
    sum(net_settled) as total_net_settled
from {{ ref('fact_reconciliation') }}
where order_created_at is not null
group by 1
