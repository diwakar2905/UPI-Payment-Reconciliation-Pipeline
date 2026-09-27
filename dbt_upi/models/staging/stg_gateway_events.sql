with source as (
    select * from {{ source('raw', 'gateway_events') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by txn_id
            order by ingested_at desc
        ) as rn
    from source
    where txn_id is not null
)

select
    txn_id,
    order_id,
    gateway_status,
    amount,
    event_time,
    event_type,
    run_date
from deduped
where rn = 1
