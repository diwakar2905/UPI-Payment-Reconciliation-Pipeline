-- Precision/recall of fact_reconciliation's discrepancy flags against the
-- generator's seeded ground truth (answer_key), one row per error type.
--
-- Only orders old enough to have fully settled are scored (same maturity
-- buffer as dbt_upi/tests/assert_reconciliation_matches_answer_key.sql) -
-- otherwise the newest few days would look like false positives for
-- missing_settlement/late_settlement just because their settlement hasn't
-- arrived yet, which would understate precision for no real reason.
--
-- Orphan payments are intentionally excluded: they have no app.orders row
-- by construction, so they're never in answer_key and can't be scored
-- against it; see assert_no_orphan_settlements for their data-quality check.

with matured_as_of as (
    select max(run_date) - interval '5 days' as cutoff
    from {{ source('raw', 'pipeline_runs') }}
),

scored as (
    select
        f.order_id,
        a.error_type as expected_error_type,
        f.is_missing_settlement,
        f.is_late_settlement,
        f.is_amount_mismatch,
        f.is_duplicate_charge,
        f.is_status_mismatch
    from {{ ref('fact_reconciliation') }} f
    join {{ ref('answer_key') }} a on a.order_id = f.order_id
    cross join matured_as_of m
    where not f.is_orphan_payment
      and coalesce(f.gateway_event_time, f.order_created_at) <= m.cutoff
),

per_type as (
    select 'missing_settlement' as error_type,
        count(*) filter (where is_missing_settlement and expected_error_type = 'missing_settlement') as true_positives,
        count(*) filter (where is_missing_settlement and expected_error_type != 'missing_settlement') as false_positives,
        count(*) filter (where not is_missing_settlement and expected_error_type = 'missing_settlement') as false_negatives
    from scored
    union all
    select 'late_settlement',
        count(*) filter (where is_late_settlement and expected_error_type = 'late_settlement'),
        count(*) filter (where is_late_settlement and expected_error_type != 'late_settlement'),
        count(*) filter (where not is_late_settlement and expected_error_type = 'late_settlement')
    from scored
    union all
    select 'amount_mismatch',
        count(*) filter (where is_amount_mismatch and expected_error_type = 'amount_mismatch'),
        count(*) filter (where is_amount_mismatch and expected_error_type != 'amount_mismatch'),
        count(*) filter (where not is_amount_mismatch and expected_error_type = 'amount_mismatch')
    from scored
    union all
    select 'duplicate_charge',
        count(*) filter (where is_duplicate_charge and expected_error_type = 'duplicate_charge'),
        count(*) filter (where is_duplicate_charge and expected_error_type != 'duplicate_charge'),
        count(*) filter (where not is_duplicate_charge and expected_error_type = 'duplicate_charge')
    from scored
    union all
    select 'status_mismatch',
        count(*) filter (where is_status_mismatch and expected_error_type = 'status_mismatch'),
        count(*) filter (where is_status_mismatch and expected_error_type != 'status_mismatch'),
        count(*) filter (where not is_status_mismatch and expected_error_type = 'status_mismatch')
    from scored
)

select
    error_type,
    true_positives,
    false_positives,
    false_negatives,
    round(true_positives::numeric / nullif(true_positives + false_positives, 0), 4) as precision,
    round(true_positives::numeric / nullif(true_positives + false_negatives, 0), 4) as recall,
    round(
        2.0 * true_positives / nullif(2 * true_positives + false_positives + false_negatives, 0),
        4
    ) as f1_score
from per_type
