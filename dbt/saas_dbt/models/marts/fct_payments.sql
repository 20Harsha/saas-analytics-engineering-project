select
    p.payment_id,
    p.user_id,
    p.subscription_id,
    p.amount,
    p.payment_date,
    p.payment_status
from {{ ref('stg_payments') }} p
join {{ ref('dim_users') }} u
    on p.user_id = u.user_id
