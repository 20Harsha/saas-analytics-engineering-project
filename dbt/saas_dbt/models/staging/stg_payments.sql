{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='append'
    )
}}

select
    payment_id,
    user_id,
    subscription_id,
    amount,
    payment_date,
    payment_status
from {{ source('raw', 'payments_raw') }}

{% if is_incremental() %}
where payment_date > (
    select coalesce(max(payment_date), '1900-01-01')
    from {{ this }}
)
{% endif %}
