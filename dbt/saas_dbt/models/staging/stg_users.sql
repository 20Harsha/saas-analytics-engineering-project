{{
    config(
        materialized = 'incremental',
        unique_key = 'user_id',
        incremental_strategy = 'merge'
    )
}}

SELECT
    user_id,
    name,
    email,
    phone,
    to_date(signup_date) as signup_date,
    country,
    acquisition_channel,
    company_size,
    to_timestamp_ntz(created_at::number / 1000000000) as created_at,
    to_timestamp_ntz(updated_at::number / 1000000000) as updated_at
FROM {{ source('raw', 'users_raw') }}

{% if is_incremental() %}
WHERE to_timestamp_ntz(updated_at::number / 1000000000) >
    (
        SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}
