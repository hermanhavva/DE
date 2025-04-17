-- models/staging/stg_payments.sql
with source as (
select
    user_id,
    STR_TO_DATE(pay_dt, '%Y-%m-%d %H:%i:%s') AS pay_dt,
    amount_usd,
    feature_type_id
from {{ source('company_x', 'payments') }}
)
select * from source s
where s.user_id is not null
and s.pay_dt is not null
and s.amount_usd is not null
and s.feature_type_id is not null

