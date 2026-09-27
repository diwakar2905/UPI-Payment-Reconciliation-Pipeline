-- Warns (returns rows) if any settlement record references a txn_id that
-- never appeared in a gateway event. A non-zero count is expected: the
-- generator's invalid_row injection can corrupt the join key on either
-- side, and late_settlement can place a settlement in a run_date that
-- hasn't been ingested yet. Severity is 'warn' rather than 'error' so this
-- surfaces as a metric, not a hard pipeline failure.

{{ config(severity='warn') }}

select s.settlement_id, s.txn_id
from {{ ref('stg_settlements') }} s
left join {{ ref('stg_gateway_events') }} g on g.txn_id = s.txn_id
where g.txn_id is null
