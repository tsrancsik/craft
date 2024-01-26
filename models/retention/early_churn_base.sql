with weeks as (
    select distinct
        user_id,
        signup_week,
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
    signup_week,
    day,
    case
        when
            documents_created > 0
        then true
        else false
    end as documents_created,
    case
        when
            document_edits > 0
        then true
        else false
    end as document_edits,
    case
        when
            document_opens > 0
        then true
        else false
    end as document_opens,
    case
        when
            document_shares > 0
        then true
        else false
    end as document_shares,
    case
        when
            comments_created > 0
        then true
        else false
    end as comments_created,
    case
        when
            reactions_created > 0
        then true
        else false
    end as reactions_created,
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
    user_id
from
    weeks
where
    day < 15
order by
    1, 2, 3