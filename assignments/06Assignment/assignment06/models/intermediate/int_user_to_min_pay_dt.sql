-- /models/intermediate/int_user_to_min_pay_dt.sql

select
  distinct p.user_id,
  min(p.pay_dt) over (partition by p.user_id) as min_pay_dt
from
  {{ ref('stg_payments') }} p

