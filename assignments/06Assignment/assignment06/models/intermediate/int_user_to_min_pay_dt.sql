-- /models/intermediate/int_user_to_min_pay_dt.sql

select
  p.user_id,
  get_min_max_value(pay_dt).min_value as min_pay_dt
from
  {{ref('stg_payments')}} p
  group by p.user_id)