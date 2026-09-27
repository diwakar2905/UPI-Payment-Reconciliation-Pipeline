with source as (
    select * from {{ source('raw', 'settlements') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by settlement_id
            order by ingested_at desc
        ) as rn
    from source
    where settlement_id is not null
)

select
    settlement_id,
    txn_id,
    gross_amount,
    mdr_fee,
    gst_on_fee,
    net_settled,
    settled_date,
    coalesce(settlement_type, 'payment') as settlement_type,
    run_date
from deduped
where rn = 1
