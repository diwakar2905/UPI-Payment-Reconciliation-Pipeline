with source as (
    select * from {{ source('raw', 'orders') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_id
            order by ingested_at desc
        ) as rn
    from source
    where order_id is not null
)

select
    order_id,
    merchant_id,
    customer_vpa,
    amount,
    status,
    created_at,
    updated_at,
    run_date
from deduped
where rn = 1
