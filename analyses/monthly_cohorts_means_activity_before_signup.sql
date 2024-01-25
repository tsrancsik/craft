select
    date_trunc('month', signup_date) as monthly_cohort,
    day,
    activity_before_signup,
    count(distinct user_id) as users,
    avg(document_created) as avg_documents_created,
    avg(document_edits) as avg_document_edits,
    avg(document_opens) as avg_document_opens,
    avg(document_shares) as avg_document_shares,
    avg(comments_created) as avg_coomments_created,
    avg(reactions_created) as avg_reactions_created
from
    {{ ref('cummulated_activities_by_age_sparse') }}
where
    day in (
        0, 1, 7, 28, 91
    )
group by
    date_trunc('month', signup_date),
    day
order by
    day,
    date_trunc('month', signup_date)