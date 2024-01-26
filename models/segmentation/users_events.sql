with data1 as (
    select
        user_id,
        signup_date,
        signup_week,
        signup_month,
        day,
        document_created,
        document_edits,
        document_opens,
        document_shares,
        comments_created,
        reactions_created
    from
        {{ ref('cumulated_activities_by_age_sparse') }}
    where
        day in (
            0, 1, 7, 28, 91, 365
        )
        and user_id in (
        '00067a81-ef46-09ea-c509-bebcb4e23415',
        '00039149-7429-9321-80d2-69a5527bb791',
        '0006a3e2-e2a4-27f1-1afe-52a68df3bd54',
        '00039149-7429-9321-80d2-69a5527bb791'
    )
),

data2 as (
    select distinct
        user_id,
        signup_date,
        signup_week,
        signup_month,
        coalesce(case when day = 0 then document_created end, 0) as document_created_0,
        coalesce(case when day = 1 then document_created end, 0) as document_created_1,
        coalesce(case when day = 7 then document_created end, 0) as document_created_7,
        coalesce(case when day = 28 then document_created end, 0) as document_created_28,
        coalesce(case when day = 91 then document_created end, 0) as document_created_91,
        coalesce(case when day = 365 then document_created end, 0) as document_created_365
    from
        data1
)

select distinct
    user_id,
    signup_date,
    signup_week,
    signup_month,
    document_created_0 as dc_0,
    document_created_1 - document_created_0 as dc_1,
    document_created_7 - document_created_1 as dc_7,
    document_created_28 - document_created_7 as dc_28,
    document_created_91 - document_created_28 as dc_91,
    document_created_365 - document_created_91 as dc_365
from
    data2
