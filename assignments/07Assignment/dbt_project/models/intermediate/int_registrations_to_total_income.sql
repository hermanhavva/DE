-- models/intermediate/int_registrations_to_total_income.sql

with user_id_to_income as
(
    select user_id, sum(amount_usd) from {{ref('stg_payments')}}
    group by user_id
)
select
    r.user_id,
    r.reg_dt,
    r.gender,
    r.age,
    r.app,
    r.country_code
  from {{ref('stg_registrations')}} r
inner join user_id_to_income on user_id_to_income.user_id = r.user_id