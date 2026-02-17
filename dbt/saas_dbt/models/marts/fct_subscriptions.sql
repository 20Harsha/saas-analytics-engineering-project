select
    s.subscription_id,
    s.user_id,
    s.plan_id,
    s.start_date,
    s.end_date,
    s.subscription_status
from {{ ref('int_subscription_details') }} s
join {{ ref('dim_users') }} u
    on s.user_id = u.user_id

