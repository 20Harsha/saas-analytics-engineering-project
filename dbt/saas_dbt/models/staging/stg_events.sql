{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='append'
    )
}}

select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    feature_name,
    device_type
from {{ source('raw', 'events_raw') }}

{% if is_incremental() %}
where event_timestamp > (
    select coalesce(max(event_timestamp), '1900-01-01')
    from {{ this }}
)
{% endif %}
