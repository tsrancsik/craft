with data as (
    select
        user_id,
        date,
        count(*) as records
    from
        {{ source('craft', 'sessions') }}
    group by
        user_id
        date
)

select
    records,
    count(*) as occurences
from
    data
group by
    records
order by
    records desc