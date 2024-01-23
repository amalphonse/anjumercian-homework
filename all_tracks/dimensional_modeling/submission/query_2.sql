#Query 2
INSERT INTO anjumercian.actors
  SELECT
    actor,
    actor_id as actorid,
    ARRAY[
    ROW(
    film,
    votes,
    rating,
    film_id
    )
    ] as films,
  CASE WHEN rating >8 THEN 'star'
  WHEN  rating > 7 and rating <=8 THEN 'good'
  WHEN rating >6 and rating <=7 THEN 'average'
  Else 'bad' 
  END as quality_class,
  CASE WHEN film_id is not null THEN TRUE
  else FALSE END as is_active,
  year as current_year
  from bootcamp.actor_films
  Where year=1922
