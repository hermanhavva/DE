-- models/marts/mart_avg_sessions_before_after_pay.sql

--with ses_per_user_before as (
--    select
--        s.user_id,
--        count(distinct s.session_number) as ses_count
--    from {{ ref('stg_sessions') }} s
--    inner join {{ ref('stg_payments') }} p
--        on p.user_id = s.user_id
--    inner join {{ ref('stg_registrations') }} r
--        on r.user_id = s.user_id
--    inner join {{ ref('int_user_to_min_pay') }} utmp
--        on utmp.user_id = s.user_id
--    where s.session_start_dt < utmp.min_pay_dt
--      and r.country_code = 'UA'
--    group by s.user_id
--),
--
--ses_per_user_after as (
--    select
--        s.user_id,
--        count(distinct s.session_number) as ses_count
--    from {{ ref('stg_sessions') }} s
--    inner join {{ ref('stg_payments') }} p
--        on p.user_id = s.user_id
--    inner join {{ ref('stg_registrations') }} r
--        on r.user_id = s.user_id
--    inner join {{ ref('int_user_to_min_pay') }} mpdpu
--        on mpdpu.user_id = s.user_id
--    where s.session_start_dt >= mpdpu.min_dt
--      and r.country_code = 'UA'
--    group by s.user_id
--)

select
    (select avg(ses_count) from {{ref('int_ses_per_user_before_pay')}}) as before_pay_avg_session,
    (select avg(ses_count) from {{ref('int_ses_per_user_after_first_pay')}}) as after_pay_avg_session
