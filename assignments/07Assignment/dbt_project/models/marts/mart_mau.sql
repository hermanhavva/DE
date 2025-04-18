-- /models/marts/mart_mau.sql

SELECT
  date_trunc('month', session_start_dt) AS month,
  COUNT(DISTINCT user_id) AS mau
FROM {{ref('stg_sessions')}}
GROUP BY month
ORDER BY month