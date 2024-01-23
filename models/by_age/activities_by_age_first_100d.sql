with day_series as (
    select
        generate_series (1, 20, 1) as day
),

users as (
    select distinct
        user_id
    from
        {{ ref('activities_by_age') }}
    where
        user_id in (
            '00067a81-ef46-09ea-c509-bebcb4e23415',
            '00039149-7429-9321-80d2-69a5527bb791',
            '0006a3e2-e2a4-27f1-1afe-52a68df3bd54'
        )
),

daily_users as (
    select
        day,
        user_id
    from
        day_series
        cross join users
),

activities as (
    select
        d.day,
        d.user_id,
        documents_created_cumsum,
        document_edits_cumsum,
        document_shares_cumsum,
        document_opens_cumsum,
        comments_created_cumsum,
        reactions_created_cumsum
    from
        daily_users as d
        left join {{ ref('activities_by_age') }} as a
            on d.user_id = a.user_id
            and d.day = a.days_from_signup
    order by
        d.user_id,
        d.day
),

document_created_partitions as (
    select
        day,
        user_id,
        documents_created_cumsum,
        count(documents_created_cumsum) over (
            partition by user_id
            order by day
        ) as document_created_partition
    from
        activities
),

document_edits_partitions as (
    select
        day,
        user_id,
        document_edits_cumsum,
        count(document_edits_cumsum) over (
            partition by user_id
            order by day
        ) as document_edits_partition
    from
        activities
)

select
    dcp.day,
    dcp.user_id,
    documents_created_cumsum,
    coalesce(
        first_value(documents_created_cumsum) over (
            partition by dcp.user_id, document_created_partition
            order by dcp.day
        ), 0
    ) as document_created,
    document_edits_cumsum,
    coalesce(
        first_value(document_edits_cumsum) over (
            partition by dep.user_id, document_edits_partition
            order by dep.day
        ), 0
    ) as document_edits
from
    document_created_partitions as dcp
    inner join document_edits_partitions as dep on dcp.user_id = dep.user_id and dcp.day = dep.day

-- first_non_null as (
--     select
--         days_from_signup,
--         user_id,
--         documents_created_cumsum,
--         case
--             when
--                 documents_created_cumsum is not null then documents_created_cumsum
--             else
--                 first_value(documents_created_cumsum) over (
--                     partition by user_id
--                     order by days_from_signup rows between unbounded preceding and current row
--                 )
--         end as documents_created,
--         document_edits_cumsum,
--         case
--             when
--                 document_edits_cumsum is not null then document_edits_cumsum
--             else
--                 first_value(document_edits_cumsum) over (
--                     partition by user_id
--                     order by days_from_signup rows between unbounded preceding and current row
--                 )
--         end as document_edits,
--         document_shares_cumsum,
--         case
--             when
--                 document_shares_cumsum is not null then document_shares_cumsum
--             else
--                 first_value(document_shares_cumsum) over (
--                     partition by user_id
--                     order by days_from_signup rows between unbounded preceding and current row
--                 )
--         end as document_shares,
--         document_opens_cumsum,
--         case
--             when
--                 document_opens_cumsum is not null then document_opens_cumsum
--             else
--                 first_value(document_opens_cumsum) over (
--                     partition by user_id
--                     order by days_from_signup rows between unbounded preceding and current row
--                 )
--         end as document_opens,
--         comments_created_cumsum,
--         case
--             when
--                 comments_created_cumsum is not null then comments_created_cumsum
--             else
--                 first_value(comments_created_cumsum) over (
--                     partition by user_id
--                     order by days_from_signup rows between unbounded preceding and current row
--                 )
--         end as comments_created,
--         reactions_created_cumsum,
--         case
--             when
--                 reactions_created_cumsum is not null then reactions_created_cumsum
--             else
--                 first_value(reactions_created_cumsum) over (
--                     partition by user_id
--                     order by days_from_signup rows between unbounded preceding and current row
--                 )
--         end as reactions_created
--     from
--         activities
--     order by
--         user_id,
--         days_from_signup
-- )

-- select
--     *
-- from
--     first_non_null
-- order by
--     user_id,
--     days_from_signup

-- select
--     days_from_signup,
--     user_id,
--     documents_created_cumsum,
--     coalesce(documents_created, 0) as documents_created
-- from
--     first_non_null
-- order by
--     user_id,
--     days_from_signup
