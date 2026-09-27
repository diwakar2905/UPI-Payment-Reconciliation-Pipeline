-- Transaction-grain payment fact: one row per gateway event (txn_id),
-- including every duplicate charge as its own row. This is the
-- "payments" side of the star schema; fact_reconciliation is order-grain
-- (one row per order) and collapses duplicates down to a single primary
-- transaction, so use this model when you need every individual gateway
-- transaction, e.g. for a payments ledger or duplicate-charge audit.

with gateway as (
    select * from {{ ref('stg_gateway_events') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

settlement_per_txn as (
    select distinct on (txn_id)
        txn_id,
        settlement_id,
        gross_amount,
        mdr_fee,
        gst_on_fee,
        net_settled,
        settled_date
    from {{ ref('stg_settlements') }}
    where settlement_type = 'payment'
    order by txn_id, settled_date asc
),

refund_per_txn as (
    select distinct on (txn_id)
        txn_id,
        settlement_id as refund_settlement_id,
        net_settled as refund_amount,
        settled_date as refund_date
    from {{ ref('stg_settlements') }}
    where settlement_type = 'refund'
    order by txn_id, settled_date asc
)

select
    g.txn_id,
    g.order_id,
    g.gateway_status,
    g.amount as gateway_amount,
    g.event_time,
    g.event_type,
    o.merchant_id,
    o.status as order_status,
    o.amount as order_amount,
    (o.order_id is null) as is_orphan_payment,
    st.settlement_id,
    st.gross_amount,
    st.mdr_fee,
    st.gst_on_fee,
    st.net_settled,
    st.settled_date,
    (st.settlement_id is not null) as is_settled,
    rf.refund_settlement_id,
    rf.refund_amount,
    rf.refund_date,
    (rf.refund_settlement_id is not null) as is_refunded
from gateway g
left join orders o on o.order_id = g.order_id
left join settlement_per_txn st on st.txn_id = g.txn_id
left join refund_per_txn rf on rf.txn_id = g.txn_id
