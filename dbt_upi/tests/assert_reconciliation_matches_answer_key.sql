-- Fails (returns rows) if fact_reconciliation disagrees with the
-- generator's ground-truth answer_key for any order.
-- Note: re-run `make dbt-seed` after `make generate` regenerates the seed,
-- otherwise this compares against a stale answer key.

with mapped_answer_key as (
    select
        order_id,
        case expected_result
            when 'match' then 'matched'
            else expected_result
        end as expected_status
    from {{ ref('answer_key') }}
)

select
    f.order_id,
    f.reconciliation_status as actual_status,
    a.expected_status
from {{ ref('fact_reconciliation') }} f
join mapped_answer_key a on a.order_id = f.order_id
where f.reconciliation_status != a.expected_status
