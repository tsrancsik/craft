with users as (
    select
        user_id,
        max("date" :: date) - signup_date :: date as last_active_day
    from
        {{ source('craft', 'sessions') }}
    group by
        user_id,
        signup_date
)

select
    s.user_id,
    signup_date :: date as signup_date,
    date_trunc('week', signup_date :: date) :: date as signup_week,
    date_trunc('month', signup_date :: date) :: date as signup_month,
    u.last_active_day,
    "date" :: date as session_date,
    "date" :: date - signup_date :: date as day,
    documents_created,
    document_edits,
    document_shares,
    document_opens,
    comments_created,
    reactions_created
from
    {{ source('craft', 'sessions') }} as s
    inner join users as u on s.user_id = u.user_id
where
    date >= signup_date