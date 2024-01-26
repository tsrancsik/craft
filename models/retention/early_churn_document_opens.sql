    select
        signup_week,
        day,
        document_opens,
        count(distinct user_id) as users
    from
        {{ ref('early_churn_base') }}
    group by
        1, 2, 3
    order by
        1, 2