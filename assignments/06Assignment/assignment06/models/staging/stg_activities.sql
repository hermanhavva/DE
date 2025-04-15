-- models/staging/stg_activities.sql

with source as (
    select
        cast(user_id as integer) as user_id,
        cast(contact_id as integer) as contact_id,
        cast(activity_type as integer) as activity_type,
        cast(dt as timestamp) as dt  -- Convert from varchar to timestamp
    from {{ source('company_x', 'activities') }}
)

select * from source