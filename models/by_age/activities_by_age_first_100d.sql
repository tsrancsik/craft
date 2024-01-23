with day_series as (
    select
        generate_series (1, 100, 1) as day
),

users as (
    select distinct
        user_id
    from
        {{ ref('activities_by_age') }}
    -- where
    --     user_id in (
    --         '00067a81-ef46-09ea-c509-bebcb4e23415',
    --         '00039149-7429-9321-80d2-69a5527bb791',
    --         '0006a3e2-e2a4-27f1-1afe-52a68df3bd54'
    --     )
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
),

document_shares_partitions as (
    select
        day,
        user_id,
        document_shares_cumsum,
        count(document_shares_cumsum) over (
            partition by user_id
            order by day
        ) as document_shares_partition
    from
        activities
),

document_opens_partitions as (
    select
        day,
        user_id,
        document_opens_cumsum,
        count(document_opens_cumsum) over (
            partition by user_id
            order by day
        ) as document_opens_partition
    from
        activities
),

comments_created_partitions as (
    select
        day,
        user_id,
        comments_created_cumsum,
        count(comments_created_cumsum) over (
            partition by user_id
            order by day
        ) as comments_created_partition
    from
        activities
),

reactions_created_partitions as (
    select
        day,
        user_id,
        reactions_created_cumsum,
        count(reactions_created_cumsum) over (
            partition by user_id
            order by day
        ) as reactions_created_partition
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
    ) as document_edits,
    document_shares_cumsum,
    coalesce(
        first_value(document_shares_cumsum) over (
            partition by dsp.user_id, document_shares_partition
            order by dsp.day
        ), 0
    ) as document_shares,
    document_opens_cumsum,
    coalesce(
        first_value(document_opens_cumsum) over (
            partition by dsp.user_id, document_opens_partition
            order by dop.day
        ), 0
    ) as document_opens,
    comments_created_cumsum,
    coalesce(
        first_value(comments_created_cumsum) over (
            partition by ccp.user_id, comments_created_partition
            order by ccp.day
        ), 0
    ) as comments_created,
    reactions_created_cumsum,
    coalesce(
        first_value(reactions_created_cumsum) over (
            partition by rcp.user_id, reactions_created_partition
            order by rcp.day
        ), 0
    ) as reactions_created

from
    document_created_partitions as dcp
    inner join document_edits_partitions as dep on dcp.user_id = dep.user_id and dcp.day = dep.day
    inner join document_shares_partitions as dsp on dcp.user_id = dsp.user_id and dcp.day = dsp.day
    inner join document_opens_partitions as dop on dcp.user_id = dop.user_id and dcp.day = dop.day
    inner join comments_created_partitions as ccp on dcp.user_id = ccp.user_id and dcp.day = ccp.day
    inner join reactions_created_partitions as rcp on dcp.user_id = rcp.user_id and dcp.day = rcp.day