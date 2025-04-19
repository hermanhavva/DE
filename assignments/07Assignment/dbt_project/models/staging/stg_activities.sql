-- models/staging/stg_activities.sql

with source as (
    select
        user_id,
        contact_id,
        activity_type,
        strptime(act_dt, '%Y-%m-%d %H:%M:%S') AS act_dt -- Convert from varchar to timestamp
    from {{ source('company_x', 'activities') }}
)

select * from source s
where s.user_id is not null
and s.contact_id is not null
and s.activity_type is not null