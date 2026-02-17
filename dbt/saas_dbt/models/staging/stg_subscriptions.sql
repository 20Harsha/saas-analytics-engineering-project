{{
    config(
        materialized='incremental',
        unique_key='subscription_id',
        incremental_strategy='merge'
    )
}}

select
    subscription_id,
    user_id,
    plan_id,
    start_date,
    end_date,
    status,
    to_timestamp_ntz(created_at::number / 1000000000) as created_at,
    to_timestamp_ntz(updated_at::number / 1000000000) as updated_at
from {{ source('raw', 'subscriptions_raw') }}

{% if is_incremental() %}
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01')
    from {{ this }}
)
{% endif %}
