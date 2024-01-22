INSERT INTO
   anjumercian.user_devices_cumulated
WITH
yesterday AS (
    SELECT
      *
    FROM
      anjumercian.user_devices_cumulated
    WHERE
      DATE = DATE('2023-01-01')
  ),
  today AS (
    SELECT 
w.user_id,
browser_type,
CAST(DATE_TRUNC('day', event_time) AS DATE) AS event_date

 FROM bootcamp.devices d
JOIN bootcamp.web_events w on d.device_id=w.device_id
    WHERE
      DATE_TRUNC('day', event_time) = DATE('2023-01-02')
    GROUP BY
      user_id,
      CAST(DATE_TRUNC('day', event_time) AS DATE)
  )

SELECT
  COALESCE(y.user_id, t.user_id) AS user_id,
browser_type,
  CASE
    WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
    ELSE ARRAY[t.event_date]
  END AS dates_active,
  DATE('2023-01-02') AS DATE
FROM
  yesterday y
  FULL OUTER JOIN today t ON y.user_id = t.user_id
