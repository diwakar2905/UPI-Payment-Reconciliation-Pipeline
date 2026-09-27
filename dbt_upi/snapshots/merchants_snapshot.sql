{% snapshot merchants_snapshot %}

{{
    config(
        target_schema='staging',
        unique_key='merchant_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select
    merchant_id,
    name,
    category,
    fee_rate,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at
from {{ source('app', 'merchants') }}

{% endsnapshot %}
