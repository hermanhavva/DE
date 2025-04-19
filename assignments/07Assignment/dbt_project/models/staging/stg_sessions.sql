-- models/staging/stg_sessions.sql

with source as (
SELECT
    user_id,
    strptime(session_start_dt, '%Y-%m-%d %H:%M:%S') AS session_start_dt,
    duration,
    session_number
FROM {{ source('company_x', 'sessions') }}
)
select * from source
WHERE
    user_id IS NOT NULL AND
    session_start_dt IS NOT NULL AND
    duration IS NOT NULL AND
    session_number IS NOT NULL
