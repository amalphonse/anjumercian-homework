INSERT INTO
  anjumercian.actors
WITH
  last_year AS (
    SELECT
      *
    FROM
      anjumercian.actors
    WHERE
      current_year=1922
  ),
  this_year AS (
    SELECT
      *
    FROM
     bootcamp.actor_films
    WHERE
      year=1923
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.film IS NULL THEN ly.films
    WHEN ty.film IS NOT NULL
    AND ly.films IS NULL THEN ARRAY[
      ROW (
        ty.film,
    	   ty.votes,
        ty.rating,
        ty.film_id
      )
    ]
    WHEN ty.film IS NOT NULL
    AND ly.films IS NOT NULL THEN ARRAY[
      ROW (
        ty.film,
    	   ty.votes,
        ty.rating,
        ty.film_id
      )
    ] || ly.films
  END AS films,
  CASE 
WHEN rating >8 THEN 'star'
  	WHEN  rating > 7 and rating <=8 THEN 'good'
  	WHEN rating >6 and rating <=7 THEN 'average'
  	Else 'bad' 
  END as quality_class,
  CASE 
WHEN film_id is not null THEN TRUE
  	ELSE FALSE 
END as is_active,
year as current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor = ty.actor
