{{ config(materialized = 'table') }}

with "28_day_retained" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) >= 28
)

select
    s.user_id,
    s.last_active_day,
    count(distinct day) as days_active,
    sum(documents_created) as documents_created,
    sum(document_edits) as document_edits,
    sum(document_opens) as document_opens,
    sum(document_shares) as document_shares,
    sum(comments_created) as comments_created,
    count(distinct
        case
            when documents_created > 0 then day
            else null
        end
    ) as documents_created_days,
    count(distinct
        case
            when document_edits > 0 then day
            else null
        end
    ) as document_edits_days,
    count(distinct
        case
            when document_opens > 0 then day
            else null
        end
    ) as document_opens_days,
    count(distinct
        case
            when document_shares > 0 then day
            else null
        end
    ) as document_shares_days,
    count(distinct
        case
            when comments_created > 0 then day
            else null
        end
    ) as comments_created_days
from
    {{ ref('stg_sessions') }} as s
    inner join "28_day_retained" as r on s.user_id = r.user_id
where
    day <= 28
group  by
    s.user_id,
    s.last_active_day
