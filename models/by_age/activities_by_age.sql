select
    user_id,
    days_from_signup,
    sum(documents_created) over (
        partition by
            user_id,
            days_from_signup
        order by
            days_from_signup
    ) as documents_created_cumsum,
    sum(document_edits) over (
        partition by
            user_id,
            days_from_signup
        order by
            days_from_signup
    ) as document_edits_cumsum,
    sum(document_shares) over (
        partition by
            user_id,
            days_from_signup
        order by
            days_from_signup
    ) as document_shares_cumsum,
    sum(document_opens) over (
        partition by
            user_id,
            days_from_signup
        order by
            days_from_signup
    ) as document_opens_cumsum,
    sum(comments_created) over (
        partition by
            user_id,
            days_from_signup
        order by
            days_from_signup
    ) as comments_created_cumsum,
    sum(reactions_created) over (
        partition by
            user_id,
            days_from_signup
        order by
            days_from_signup
    ) as reactions_created_cumsum
from
    {{ ref('stg_sessions') }}