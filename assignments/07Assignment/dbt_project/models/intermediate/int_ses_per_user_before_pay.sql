-- /models/intermediate/int_ses_per_user_before_pay.sql

select
    s.user_id,
    count(distinct s.session_number) as ses_count
from {{ ref('stg_sessions') }} s
inner join {{ ref('int_user_to_min_pay_dt') }} utmp
    on utmp.user_id = s.user_id
where s.session_start_dt < utmp.min_pay_dt
group by s.user_id
