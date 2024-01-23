#query 1
CREATE TABLE anjumercian.actors (
   actor VARCHAR,
   actor_id VARCHAR,
   films ARRAY(
        ROW(
        film VARCHAR,
        votes INTEGER,
        rating DOUBLE ,
        filmid VARCHAR
        )
),
    quality_class VARCHAR,
    is_active BOOLEAN,
 current_year INTEGER   
  )
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
