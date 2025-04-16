-- models/marts/mart_avg_sessions_before_after_pay.sql

select
    (select avg(ses_count) from {{ref('int_ses_per_user_before_pay')}}) as before_pay_avg_session,
    (select avg(ses_count) from {{ref('int_ses_per_user_after_first_pay')}}) as after_pay_avg_session
