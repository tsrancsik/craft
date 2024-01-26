select
    user_id,
    signup_date :: date as signup_date,
    date_trunc('week', signup_date :: date) :: date as signup_week,
    date_trunc('month', signup_date :: date) :: date as signup_month,
    "date" :: date as session_date,
    "date" :: date - signup_date :: date as day,
    documents_created,
    document_edits,
    document_shares,
    document_opens,
    comments_created,
    reactions_created
from
    {{ source('craft', 'sessions') }}
where
    date >= (
        select
            min(signup_date) as first_signup
        from
            {{ source('craft', 'sessions') }}
    )