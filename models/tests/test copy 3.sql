select * from {{ ref('activities_by_age_sparse') }}
where
    user_id in (
        '00067a81-ef46-09ea-c509-bebcb4e23415',
        '00039149-7429-9321-80d2-69a5527bb791',
        '0006a3e2-e2a4-27f1-1afe-52a68df3bd54',
        '00039149-7429-9321-80d2-69a5527bb791'
    )
order by user_id, day