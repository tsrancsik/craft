with "4weeks" as (
    select distinct
        user_id,
        signup_month,
        days_from_signup,
        sum(documents_created) over (
            partition by user_id
            order by days_from_signup
            range between 27 preceding and current row
        ) as document_created_4w
    from
        -- we need a table filled with zeros
        {{ ref('stg_sessions') }}
    where
        days_from_signup in (28, 56, 84)
)
select
    user_id,
    signup_month,
    days_from_signup,
    case
        when document_created_4w > 0 then true
        else false
    end as retained
from
    "4weeks"
order by
    1, 2, 3