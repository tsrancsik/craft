with weeks as (
    select distinct
        user_id,
        signup_week,
        day,
        sum(documents_created) over (
            partition by user_id
            order by day
            range between 7 preceding and current row
        ) as documents_created,
        sum(document_edits) over (
            partition by user_id
            order by day
            range between 7 preceding and current row
        ) as document_edits,
        sum(document_opens) over (
            partition by user_id
            order by day
            range between 7 preceding and current row
        ) as document_opens,
        sum(document_shares) over (
            partition by user_id
            order by day
            range between 7 preceding and current row
        ) as document_shares,
        sum(comments_created) over (
            partition by user_id
            order by day
            range between 7 preceding and current row
        ) as comments_created,
        sum(reactions_created) over (
            partition by user_id
            order by day
            range between 7 preceding and current row
        ) as reactions_created
    from
        {{ ref('activities_by_age_sparse') }}
)
select
    signup_week,
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
    day in (7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84)
group by
    1, 2, 3
order by
    1, 2, 3