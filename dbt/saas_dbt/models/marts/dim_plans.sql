select
    plan_id,
    plan_name,
    monthly_price,
    billing_cycle
from {{ ref('stg_plans') }}
