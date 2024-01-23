select
    *
from
    crosstab ('
        select
            user_id,
            days_from_signup,
            documents_created_cumsum,
            document_edits_cumsum,
            document_shares_cumsum,
            document_opens_cumsum,
            comments_created_cumsum,
            reactions_created_cumsum
        from
            {{ ref('activities_by_age') }}
        where
            days_from_signup <= 100
        order by
            user_id,
            days_from_signup
    ')