-- models/staging/stg_sessions.sql

SELECT
    user_id,
    STR_TO_DATE(session_start_dt, '%Y-%m-%d %H:%i:%s') AS session_start_dt,
    duration,
    session_number
FROM {{ source('company_x', 'sessions') }}
