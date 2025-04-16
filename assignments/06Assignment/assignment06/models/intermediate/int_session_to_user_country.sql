-- models/intermediate/int_session_to_country.sql

with user_id_to_country_code as (
    select user_id, country_code from {{ ref('stg_registrations') }}
),
sessions_query as (
    select user_id, session_start_dt, duration, session_number from {{ ref('stg_sessions') }}
)
select 
    s.user_id,
    s.session_start_dt,
    s.duration,
    s.session_number,
    r.country_code
from sessions_query s
inner join user_id_to_country_code r on r.user_id = s.user_id
