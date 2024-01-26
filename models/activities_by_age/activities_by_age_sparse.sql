with day_series as (
    select
        generate_series (0, 100, 1) as day
),

users as (
    select distinct
        user_id,
        signup_date,
        signup_week,
        signup_month,
        case
            when min(day) < 0 then true
            else false
        end as activity_before_signup
    from
        {{ ref('stg_sessions') }}
    -- where
    --     user_id in (
    --         '00067a81-ef46-09ea-c509-bebcb4e23415',
    --         '00039149-7429-9321-80d2-69a5527bb791',
    --         '0006a3e2-e2a4-27f1-1afe-52a68df3bd54'
    --     )
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
        signup_month,
        activity_before_signup
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
        d.activity_before_signup,
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

select * from activities
-- where
--     user_id in (
--         '00067a81-ef46-09ea-c509-bebcb4e23415',
--         '00039149-7429-9321-80d2-69a5527bb791',
--         '0006a3e2-e2a4-27f1-1afe-52a68df3bd54',
--         '00039149-7429-9321-80d2-69a5527bb791'
--     )
order by user_id, day