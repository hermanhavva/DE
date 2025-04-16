-- models/staging/stg_registrations.sql

with source as (
SELECT
    user_id,
    STR_TO_DATE(reg_dt, '%Y-%m-%d %H:%i:%s') AS reg_dt,
    gender,
    age,
    app,
    country_code
FROM {{ source('company_x', 'registrations') }}
)
select * from source
WHERE
    user_id IS NOT NULL AND
    reg_dt IS NOT NULL AND
    gender IS NOT NULL AND
    age IS NOT NULL AND
    app IS NOT NULL AND
    country_code IS NOT NULL AND
    app != "unknown" AND
    age > 0
