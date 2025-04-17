-- /models/marts/mart_mau.sql

SELECT
  DATE_FORMAT(session_start_dt, '%Y-%m-01') AS month,
  COUNT(DISTINCT user_id) AS mau
FROM {{ref('stg_sessions')}}
GROUP BY month
ORDER BY month