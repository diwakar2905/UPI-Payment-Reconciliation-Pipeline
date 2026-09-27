-- Core reconciliation fact: one row per order, plus orphan gateway payments
-- that have no matching app.orders row. Flags every discrepancy class the
-- generator injects (see generator/config.yaml).

with orders as (
    select * from {{ ref('stg_orders') }}
),

gateway as (
    select * from {{ ref('stg_gateway_events') }}
),

settlements as (
    select * from {{ ref('stg_settlements') }}
),

gateway_per_order as (
    select
        order_id,
        count(*) filter (where gateway_status = 'success') as success_event_count,
        count(*) as event_count
    from gateway
    group by order_id
),

primary_gateway_event as (
    -- earliest successful (or, failing that, earliest) event per order
    select distinct on (order_id)
        order_id,
        txn_id,
        gateway_status,
        amount as gateway_amount,
        event_time
    from gateway
    order by order_id, (gateway_status = 'success') desc, event_time asc
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
    from settlements
    order by txn_id, settled_date asc
),

matched as (
    select
        o.order_id,
        o.merchant_id,
        o.amount as order_amount,
        o.status as order_status,
        o.created_at as order_created_at,
        pg.txn_id,
        pg.gateway_status,
        pg.gateway_amount,
        pg.event_time as gateway_event_time,
        coalesce(gpo.event_count, 0) as event_count,
        st.settlement_id,
        st.gross_amount,
        st.net_settled,
        st.settled_date,
        false as is_orphan_payment
    from orders o
    left join gateway_per_order gpo on gpo.order_id = o.order_id
    left join primary_gateway_event pg on pg.order_id = o.order_id
    left join settlement_per_txn st on st.txn_id = pg.txn_id
),

orphans as (
    select
        g.order_id,
        cast(null as varchar) as merchant_id,
        cast(null as numeric) as order_amount,
        cast(null as varchar) as order_status,
        cast(null as timestamptz) as order_created_at,
        g.txn_id,
        g.gateway_status,
        g.amount as gateway_amount,
        g.event_time as gateway_event_time,
        1 as event_count,
        st.settlement_id,
        st.gross_amount,
        st.net_settled,
        st.settled_date,
        true as is_orphan_payment
    from gateway g
    left join settlement_per_txn st on st.txn_id = g.txn_id
    where g.order_id not in (select order_id from orders)
      and g.gateway_status = 'success'
),

unioned as (
    select * from matched
    union all
    select * from orphans
),

flagged as (
    select
        *,
        (event_count > 1) as is_duplicate_charge,
        (order_status = 'paid' and (gateway_status is null or gateway_status = 'failed'))
            or (order_status = 'failed' and gateway_status = 'success') as is_status_mismatch,
        (order_status = 'paid' and gateway_status = 'success' and settlement_id is null) as is_missing_settlement,
        (settlement_id is not null and order_amount is not null and gross_amount != order_amount) as is_amount_mismatch,
        -- measured from the gateway confirmation, not order_created_at: a
        -- pending order can legitimately resolve (and settle) days after
        -- it was first created, which isn't a "late settlement".
        (settlement_id is not null and settled_date is not null and gateway_event_time is not null
            and settled_date > (gateway_event_time::date + 1)) as is_late_settlement
    from unioned
)

select
    *,
    case
        when order_status = 'created' then 'pending'
        when is_orphan_payment then 'discrepancy'
        when is_duplicate_charge or is_status_mismatch or is_missing_settlement
            or is_amount_mismatch or is_late_settlement then 'discrepancy'
        when order_status = 'paid' and gateway_status = 'success' and settlement_id is not null then 'matched'
        when order_status in ('failed', 'refunded') and gateway_status = 'failed' then 'matched'
        else 'discrepancy'
    end as reconciliation_status
from flagged
