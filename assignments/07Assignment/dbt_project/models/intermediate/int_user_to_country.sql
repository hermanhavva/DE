-- /models/intermediate/int_user_to_country.sql

select
    user_id,
    country_code
from {{ref('stg_registrations')}}