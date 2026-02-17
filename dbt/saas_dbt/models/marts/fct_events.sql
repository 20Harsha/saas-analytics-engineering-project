select
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_timestamp
from {{ ref('stg_events') }} e
join {{ ref('dim_users') }} u
    on e.user_id = u.user_id
