select
    user_id,
    signup_date,
    signup_week,
    signup_month,
    days_from_signup,
    sum(documents_created) over (
        partition by
            user_id
        order by
            days_from_signup
    ) as documents_created_cumsum,
    sum(document_edits) over (
        partition by
            user_id
        order by
            days_from_signup
        rows between unbounded preceding and current row
    ) as document_edits_cumsum,
    sum(document_shares) over (
        partition by
            user_id
        order by
            days_from_signup
        rows between unbounded preceding and current row
    ) as document_shares_cumsum,
    sum(document_opens) over (
        partition by
            user_id
        order by
            days_from_signup
        rows between unbounded preceding and current row
    ) as document_opens_cumsum,
    sum(comments_created) over (
        partition by
            user_id
        order by
            days_from_signup
        rows between unbounded preceding and current row
    ) as comments_created_cumsum,
    sum(reactions_created) over (
        partition by
            user_id
        order by
            days_from_signup
        rows between unbounded preceding and current row
    ) as reactions_created_cumsum
from
    {{ ref('stg_sessions') }}
order by
    user_id,
    days_from_signup