with data as (
    select
        count(distinct user_id) as users,
        count(*) as records,
        count(*) / count(distinct user_id) as avg_sessions_per_user,
        max(day) as max_days_from_signup,
        min(day) as min_days_from_signup
    from
        {{ ref('stg_sessions') }}
)

select
    *
from
    data