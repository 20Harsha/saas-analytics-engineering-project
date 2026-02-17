select
    plan_id,
    plan_name,
    monthly_price,
    billing_cycle,
    to_timestamp_ntz(created_at::number / 1000000000) as created_at
from {{ source('raw', 'plans_raw') }}
