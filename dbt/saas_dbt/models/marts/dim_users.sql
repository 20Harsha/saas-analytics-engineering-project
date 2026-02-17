select
    user_id,
    name,
    {{ mask_email('email') }} as masked_email,
    {{ mask_phone('phone') }} as masked_phone,
    country,
    acquisition_channel,
    company_size,
    signup_date
from {{ ref('stg_users') }}
where {{ exclude_test_users('name') }}