-- One row per calendar day: reconciliation health at a glance, the
-- natural top-level chart for a BI dashboard's daily trend view.

with pipeline_progress as (
    -- "as of" reference for aging, so this doesn't depend on wall-clock
    -- time: how far this pipeline has actually loaded data.
    select max(run_date) as as_of_date
    from {{ source('raw', 'pipeline_runs') }}
)

select
    f.order_created_at::date as report_date,
    count(*) as total_orders,
    count(*) filter (where f.reconciliation_status = 'matched') as matched_count,
    count(*) filter (where f.reconciliation_status = 'discrepancy') as discrepancy_count,
    count(*) filter (where f.reconciliation_status = 'refunded') as refunded_count,
    count(*) filter (where f.reconciliation_status = 'pending') as pending_count,
    round(
        100.0 * count(*) filter (where f.reconciliation_status = 'discrepancy')
            / nullif(count(*), 0),
        2
    ) as discrepancy_rate_pct,
    sum(f.order_amount) filter (where f.reconciliation_status = 'discrepancy') as discrepancy_amount,
    -- money that's supposed to have settled (payment succeeded) but has no
    -- settlement row yet, whether that's a real gap or just not arrived yet
    sum(f.order_amount) filter (
        where f.settlement_id is null
          and f.order_status in ('paid', 'refunded')
          and f.gateway_status = 'success'
    ) as unsettled_amount,
    sum(f.net_settled) as total_net_settled,
    round(avg(pp.as_of_date - f.order_created_at::date) filter (
        where f.reconciliation_status = 'discrepancy'
    ), 1) as avg_discrepancy_age_days,
    max(pp.as_of_date - f.order_created_at::date) filter (
        where f.reconciliation_status = 'discrepancy'
    ) as max_discrepancy_age_days
from {{ ref('fact_reconciliation') }} f
cross join pipeline_progress pp
where f.order_created_at is not null
group by 1
