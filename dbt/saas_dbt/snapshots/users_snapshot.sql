{% snapshot users_snapshot %}

{{
    config(
        target_schema='ANALYTICS_SNAPSHOTS',
        unique_key='user_id',
        strategy='check',
        check_cols=[
            'email','country',
            'acquisition_channel',
            'company_size'
        ]
    )
}}

select
    user_id,
    email,
    country,
    acquisition_channel,
    company_size
from {{ ref('stg_users') }}

{% endsnapshot %}
