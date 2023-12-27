#query 5
CREATE TABLE
  anjumercian.hosts_cumulated (
    Host VARCHAR,
    host_activity_date_list ARRAY (DATE),
    DATE DATE
  )
WITH
  (FORMAT = 'PARQUET', partitioning = host)
