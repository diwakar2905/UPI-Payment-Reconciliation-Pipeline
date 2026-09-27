with snapshot as (
    select * from {{ ref('merchants_snapshot') }}
)

select
    merchant_id,
    name,
    category,
    fee_rate,
    created_at,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    (dbt_valid_to is null) as is_current
from snapshot
