with weeks as (
    select distinct
        user_id,
        day,
        documents_created,
        document_edits,
        document_opens,
        document_shares,
        comments_created,
        reactions_created
    from
        {{ ref('activities_by_age_sparse') }}
)
select
    day,
    case
        when
            documents_created > 0
            or document_edits > 0
            or document_opens > 0
            or document_shares > 0
            or comments_created > 0
            or reactions_created > 0
        then true
        else false
    end as retained,
    count(distinct user_id) as users
from
    weeks
where
    day < 15
group by
    1, 2
order by
    1, 2