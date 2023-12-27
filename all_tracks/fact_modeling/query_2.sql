#Query 2


CREATE TABLE
  anjumercian.user_devices_cumulated (
    user_id BIGINT,
         browser_type VARCHAR,
    dates_active ARRAY (DATE),
    DATE DATE
  )
WITH
  (FORMAT = 'PARQUET', partitioning = ARRAY['date'])
