-- models/staging/stg_registrations.sql

SELECT
    user_id,
    STR_TO_DATE(reg_dt, '%Y-%m-%d %H:%i:%s') AS reg_dt,
    gender,
    age,
    app,
    country_code
FROM {{ source('company_x', 'registrations') }}
