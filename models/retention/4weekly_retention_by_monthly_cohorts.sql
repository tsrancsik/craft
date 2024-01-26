with "4weeks" as (
    select distinct
        user_id,
        signup_month,
        day,
        sum(documents_created) over (
            partition by user_id
            order by day
            range between 28 preceding and current row
        ) as documents_created_4w,
        sum(document_edits) over (
            partition by user_id
            order by day
            range between 28 preceding and current row
        ) as document_edits_4w,
        sum(document_opens) over (
            partition by user_id
            order by day
            range between 28 preceding and current row
        ) as document_opens_4w,
        sum(document_shares) over (
            partition by user_id
            order by day
            range between 28 preceding and current row
        ) as document_shares_4w,
        sum(comments_created) over (
            partition by user_id
            order by day
            range between 28 preceding and current row
        ) as comments_created_4w,
        sum(reactions_created) over (
            partition by user_id
            order by day
            range between 28 preceding and current row
        ) as reactions_created_4w
    from
        {{ ref('activities_by_age_sparse') }}
)
select
    signup_month,
    day,
    case
        when
            documents_created_4w > 0
            or document_edits_4w > 0
            or document_opens_4w > 0
            or document_shares_4w > 0
            or comments_created_4w > 0
            or reactions_created_4w > 0
        then true
        else false
    end as retained,
    count(distinct user_id) as users
from
    "4weeks"
where
    day in (28, 56, 84, 112, 140, 168, 196, 224, 252, 280, 308, 336, 364)
group by
    1, 2, 3
order by
    1, 2, 3