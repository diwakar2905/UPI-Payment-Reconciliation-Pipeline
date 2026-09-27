-- Fails (returns rows) if a day's order volume swings more than 50% versus
-- the day before it - a sudden drop usually means an upstream extract/load
-- failure, and a sudden spike usually means a duplicate load. The first
-- day in the dataset has no prior day to compare against, so it's excluded.

{{ config(severity='warn') }}

with daily as (
    select
        report_date,
        total_orders,
        lag(total_orders) over (order by report_date) as prior_day_orders
    from {{ ref('daily_reconciliation_summary') }}
)

select
    report_date,
    total_orders,
    prior_day_orders,
    round(100.0 * (total_orders - prior_day_orders) / nullif(prior_day_orders, 0), 1) as pct_change
from daily
where prior_day_orders is not null
  and abs(total_orders - prior_day_orders) > 0.5 * prior_day_orders
