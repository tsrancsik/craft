{{ config(materialized = 'table') }}

with "0_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) = 0
),

"1_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) <= 1
),

"7_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) <= 7
),

"14_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) <= 14
),

"28_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) <= 28
),

"91_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) <= 91
),

"183_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) <= 183
),

"335_day_churn" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) <= 335
),

"over_335_day_retained" as (
select
    user_id,
    max(day) as max_day
from
    {{ ref('stg_sessions') }}
group by
    user_id
having
    max(day) > 335
),

all_users as (
    select distinct
        user_id
    from
        {{ ref('stg_sessions') }}
)

select
    count(distinct all_users.user_id) as all_users,
    count(distinct "0_day_churn".user_id) as "0_day_churned",
    count(distinct all_users.user_id) - count(distinct "0_day_churn".user_id) as "0_day_retained",
    count(distinct "1_day_churn".user_id) as "1_day_churned",
    count(distinct all_users.user_id)
    - count(distinct "1_day_churn".user_id) as "1_day_retained",
    count(distinct "7_day_churn".user_id) as "7_day_churned",
    count(distinct all_users.user_id)
    - count(distinct "7_day_churn".user_id) as "7_day_retained",
    count(distinct "14_day_churn".user_id) as "14_day_churned",
    count(distinct all_users.user_id)
    - count(distinct "14_day_churn".user_id) as "14_day_retained",
    count(distinct "28_day_churn".user_id) as "28_day_churned",
    count(distinct all_users.user_id)
    - count(distinct "28_day_churn".user_id) as "28_day_retained",
    count(distinct "91_day_churn".user_id) as "91_day_churned",
    count(distinct all_users.user_id)
    - count(distinct "91_day_churn".user_id) as "91_day_retained",
    count(distinct "183_day_churn".user_id) as "183_day_churned",
    count(distinct all_users.user_id)
    - count(distinct "183_day_churn".user_id) as "183_day_retained",
    count(distinct "335_day_churn".user_id) as "335_day_churned",
    count(distinct all_users.user_id)
    - count(distinct "335_day_churn".user_id) as "335_day_retained"
from
    all_users
    left join "0_day_churn" on all_users.user_id = "0_day_churn".user_id
    left join "1_day_churn" on all_users.user_id = "1_day_churn".user_id
    left join "7_day_churn" on all_users.user_id = "7_day_churn".user_id
    left join "14_day_churn" on all_users.user_id = "14_day_churn".user_id
    left join "28_day_churn" on all_users.user_id = "28_day_churn".user_id
    left join "91_day_churn" on all_users.user_id = "91_day_churn".user_id
    left join "183_day_churn" on all_users.user_id = "183_day_churn".user_id
    left join "335_day_churn" on all_users.user_id = "335_day_churn".user_id
    left join "over_335_day_retained" on all_users.user_id = "over_335_day_retained".user_id