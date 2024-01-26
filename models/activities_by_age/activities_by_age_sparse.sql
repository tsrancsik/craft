{{ config(materialized='view') }}

with day_series as (
    select
        generate_series (0, 365, 1) as day
),

users as (
    select distinct
        user_id,
        signup_date,
        signup_week,
        signup_month
    from
        {{ ref('stg_sessions') }}
    group by
        user_id,
        signup_date,
        signup_week,
        signup_month
),

daily_users as (
    select
        day,
        user_id,
        signup_date,
        signup_week,
        signup_month
    from
        day_series
        cross join users
),

activities as (
    select
        d.day,
        d.user_id,
        d.signup_date,
        d.signup_month,
        d.signup_week,
        coalesce(documents_created, 0) as documents_created,
        coalesce(document_edits, 0) as document_edits,
        coalesce(document_shares, 0) as document_shares,
        coalesce(document_opens, 0) as document_opens,
        coalesce(comments_created, 0) as comments_created,
        coalesce(reactions_created, 0) as reactions_created
    from
        daily_users as d
        left join {{ ref('stg_sessions') }} as a
            on d.user_id = a.user_id
            and d.day = a.day
    order by
        d.user_id,
        d.day
)

select
    *
from activities
order by
    user_id,
    day