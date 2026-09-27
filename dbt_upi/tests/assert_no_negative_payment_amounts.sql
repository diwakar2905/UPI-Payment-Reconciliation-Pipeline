-- Fails (returns rows) if any order amount or gateway payment amount is
-- negative. Settlements are exempt: a refund settlement is *supposed* to
-- carry a negative gross_amount/net_settled (see stg_settlements /
-- generator's settlement_type='refund'), so that's checked separately by
-- assert_reconciliation_matches_answer_key rather than here.

select 'order' as source, order_id as id, amount
from {{ ref('stg_orders') }}
where amount < 0

union all

select 'gateway' as source, txn_id as id, amount
from {{ ref('stg_gateway_events') }}
where amount < 0
