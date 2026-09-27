-- Fails (returns rows) if fact_reconciliation disagrees with the
-- generator's ground-truth answer_key for any *matured* order.
-- Note: re-run `make dbt-seed` after `make generate` regenerates the seed,
-- otherwise this compares against a stale answer key.
--
-- Settlement lags payment by T+1, and a refund lags its settlement by a
-- further 1-3 days, so any order from the last ~5 days of an ingestion
-- window is expected to still show as "not yet settled" even though
-- nothing is wrong - that's real settlement latency, not a discrepancy.
-- We only hold orders to account once they've had time to fully resolve,
-- measured against the newest run_date this pipeline has actually loaded
-- (not wall-clock time, so this holds for both a live and a backtested run).

with mapped_answer_key as (
    select
        order_id,
        case expected_result
            when 'match' then 'matched'
            else expected_result
        end as expected_status
    from {{ ref('answer_key') }}
),

matured_as_of as (
    select max(run_date) - interval '5 days' as cutoff
    from {{ source('raw', 'pipeline_runs') }}
)

select
    f.order_id,
    f.reconciliation_status as actual_status,
    a.expected_status
from {{ ref('fact_reconciliation') }} f
join mapped_answer_key a on a.order_id = f.order_id
cross join matured_as_of
where f.reconciliation_status != a.expected_status
  and coalesce(f.gateway_event_time, f.order_created_at) <= matured_as_of.cutoff
