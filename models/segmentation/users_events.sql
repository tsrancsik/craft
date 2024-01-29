{{ config(materialized='table') }}

with data1 as (
    select
        user_id,
        signup_date,
        signup_week,
        signup_month,
        last_active_day,
        day,
        documents_created,
        document_edits,
        document_opens,
        document_shares,
        comments_created,
        reactions_created,
        documents_created
        + document_edits
        + document_opens
        + document_shares
        + comments_created
        + reactions_created as all_events
    from
        {{ ref('cumulated_activities_by_age_sparse') }}
    where
        day in (
            0, 1, 7, 28, 91, 365
        )
),

data2 as (
    select distinct
        user_id,
        signup_date,
        signup_week,
        signup_month,
        last_active_day,
        max(coalesce(case when day = 0 then documents_created end, 0)) as documents_created_0,
        max(coalesce(case when day = 1 then documents_created end, 0)) as documents_created_1,
        max(coalesce(case when day = 7 then documents_created end, 0)) as documents_created_7,
        max(coalesce(case when day = 28 then documents_created end, 0)) as documents_created_28,
        max(coalesce(case when day = 91 then documents_created end, 0)) as documents_created_91,
        max(coalesce(case when day = 365 then documents_created end, 0)) as documents_created_365,
        max(coalesce(case when day = 0 then document_edits end, 0)) as document_edits_0,
        max(coalesce(case when day = 1 then document_edits end, 0)) as document_edits_1,
        max(coalesce(case when day = 7 then document_edits end, 0)) as document_edits_7,
        max(coalesce(case when day = 28 then document_edits end, 0)) as document_edits_28,
        max(coalesce(case when day = 91 then document_edits end, 0)) as document_edits_91,
        max(coalesce(case when day = 365 then document_edits end, 0)) as document_edits_365,
        max(coalesce(case when day = 0 then document_opens end, 0)) as document_opens_0,
        max(coalesce(case when day = 1 then document_opens end, 0)) as document_opens_1,
        max(coalesce(case when day = 7 then document_opens end, 0)) as document_opens_7,
        max(coalesce(case when day = 28 then document_opens end, 0)) as document_opens_28,
        max(coalesce(case when day = 91 then document_opens end, 0)) as document_opens_91,
        max(coalesce(case when day = 365 then document_opens end, 0)) as document_opens_365,
        max(coalesce(case when day = 0 then document_shares end, 0)) as document_shares_0,
        max(coalesce(case when day = 1 then document_shares end, 0)) as document_shares_1,
        max(coalesce(case when day = 7 then document_shares end, 0)) as document_shares_7,
        max(coalesce(case when day = 28 then document_shares end, 0)) as document_shares_28,
        max(coalesce(case when day = 91 then document_shares end, 0)) as document_shares_91,
        max(coalesce(case when day = 365 then document_shares end, 0)) as document_shares_365,
        max(coalesce(case when day = 0 then comments_created end, 0)) as comments_created_0,
        max(coalesce(case when day = 1 then comments_created end, 0)) as comments_created_1,
        max(coalesce(case when day = 7 then comments_created end, 0)) as comments_created_7,
        max(coalesce(case when day = 28 then comments_created end, 0)) as comments_created_28,
        max(coalesce(case when day = 91 then comments_created end, 0)) as comments_created_91,
        max(coalesce(case when day = 365 then comments_created end, 0)) as comments_created_365,
        max(coalesce(case when day = 0 then all_events end, 0)) as all_events_0,
        max(coalesce(case when day = 1 then all_events end, 0)) as all_events_1,
        max(coalesce(case when day = 7 then all_events end, 0)) as all_events_7,
        max(coalesce(case when day = 28 then all_events end, 0)) as all_events_28,
        max(coalesce(case when day = 91 then all_events end, 0)) as all_events_91,
        max(coalesce(case when day = 365 then all_events end, 0)) as all_events_365
        -- max(coalesce(case when day = 0 then reactions_created end, 0)) as reactions_created_0,
        -- max(coalesce(case when day = 1 then reactions_created end, 0)) as reactions_created_1,
        -- max(coalesce(case when day = 7 then reactions_created end, 0)) as reactions_created_7,
        -- max(coalesce(case when day = 28 then reactions_created end, 0)) as reactions_created_28,
        -- max(coalesce(case when day = 91 then reactions_created end, 0)) as reactions_created_91,
        -- max(coalesce(case when day = 365 then reactions_created end, 0)) as reactions_created_365
    from
        data1
    group by
        user_id,
        signup_date,
        signup_week,
        signup_month,
        last_active_day
),

data3 as (
    select distinct
        user_id,
        signup_date,
        signup_week,
        signup_month,
        last_active_day,
        documents_created_0 as dc_0,
        documents_created_1 - documents_created_0 as dc_1,
        documents_created_7 - documents_created_1 as dc_7,
        documents_created_28 - documents_created_7 as dc_28,
        documents_created_91 - documents_created_28 as dc_91,
        documents_created_365 - documents_created_91 as dc_365,
        document_edits_0 as de_0,
        document_edits_1 - document_edits_0 as de_1,
        document_edits_7 - document_edits_1 as de_7,
        document_edits_28 - document_edits_7 as de_28,
        document_edits_91 - document_edits_28 as de_91,
        document_edits_365 - document_edits_91 as de_365,
        document_opens_0 as do_0,
        document_opens_1 - document_opens_0 as do_1,
        document_opens_7 - document_opens_1 as do_7,
        document_opens_28 - document_opens_7 as do_28,
        document_opens_91 - document_opens_28 as do_91,
        document_opens_365 - document_opens_91 as do_365,
        document_shares_0 as ds_0,
        document_shares_1 - document_shares_0 as ds_1,
        document_shares_7 - document_shares_1 as ds_7,
        document_shares_28 - document_shares_7 as ds_28,
        document_shares_91 - document_shares_28 as ds_91,
        document_shares_365 - document_shares_91 as ds_365,
        comments_created_0 as cc_0,
        comments_created_1 - comments_created_0 as cc_1,
        comments_created_7 - comments_created_1 as cc_7,
        comments_created_28 - comments_created_7 as cc_28,
        comments_created_91 - comments_created_28 as cc_91,
        comments_created_365 - comments_created_91 as cc_365,
        all_events_0 as ae_0,
        all_events_1 - all_events_0 as ae_1,
        all_events_7 - all_events_1 as ae_7,
        all_events_28 - all_events_7 as ae_28,
        all_events_91 - all_events_28 as ae_91,
        all_events_365 - all_events_91 as ae_365
        -- reactions_created_0 as rc_0,
        -- reactions_created_1 - reactions_created_0 as rc_1,
        -- reactions_created_7 - reactions_created_1 as rc_7,
        -- reactions_created_28 - reactions_created_7 as rc_28,
        -- reactions_created_91 - reactions_created_28 as rc_91,
        -- reactions_created_365 - reactions_created_91 as rc_365
    from
        data2
)

select
    *,
    case when ae_0 > 1 then 1 else 0 end as r_0,
    case when ae_1 > 1 then 1 else 0 end as r_1,
    case when ae_7 > 1 then 1 else 0 end as r_7,
    case when ae_28 > 1 then 1 else 0 end as r_28,
    case when ae_91 > 1 then 1 else 0 end as r_91,
    case when ae_365 > 1 then 1 else 0 end as r_365
from
    data3