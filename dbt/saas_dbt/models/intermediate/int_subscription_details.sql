select
    s.subscription_id,
    s.user_id,
    u.country,
    u.acquisition_channel,
    u.company_size,

    s.plan_id,
    p.plan_name,
    p.monthly_price,
    p.billing_cycle,

    s.start_date,
    s.end_date,
    s.status as subscription_status,

    s.created_at as subscription_created_at

from {{ ref('stg_subscriptions') }} s
left join {{ ref('stg_users') }} u
    on s.user_id = u.user_id
left join {{ ref('stg_plans') }} p
    on s.plan_id = p.plan_id
