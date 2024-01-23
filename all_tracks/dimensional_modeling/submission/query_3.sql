#query 3
CREATE TABLE
  anjumercian.actors_history_scd (
    actor VARCHAR,
    is_active BOOLEAN,
    quality_class VARCHAR,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
  )
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
