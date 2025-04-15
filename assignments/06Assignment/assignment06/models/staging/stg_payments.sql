-- models/staging/stg_payments.sql

select
    user_id,
    STR_TO_DATE(pay_dt, '%Y-%m-%d %H:%i:%s') AS pay_dt,
    amount_usd,
    feature_type_id
from {{ source('company_x', 'payments') }}
