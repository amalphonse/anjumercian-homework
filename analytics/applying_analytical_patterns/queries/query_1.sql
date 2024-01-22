WITH
  last_season AS (
    SELECT
      *
    FROM
      bootcamp.nba_players
    WHERE
      current_season = 2000
  ),
  this_season AS (
    SELECT
      *
    FROM
      bootcamp.nba_players
    WHERE
      current_season = 2001
  )
 
SELECT
  COALESCE(ls.player_name, ts.player_name) AS player_name,
  COALESCE(ls.height, ts.height) AS height,
  COALESCE(ls.college, ts.college) AS college,
  COALESCE(ls.country, ts.country) AS country,
  COALESCE(ls.draft_year, ts.draft_year) AS draft_year,
  COALESCE(ls.draft_round, ts.draft_round) AS draft_round,
  COALESCE(ls.draft_number, ts.draft_number) AS draft_number,
  COALESCE(ls.seasons,ts.seasons) AS seasons,
  COALESCE(ls.is_active,ts.is_active) AS is_active,
  COALESCE(ls.current_season,ts.current_season) AS current_season,
  
  CASE 
  WHEN ls.years_since_last_active=0 and ts.is_active=true THEN 'NEW'
  WHEN ls.is_active = true and ts.is_active =false THEN 'Retired'
   WHEN ls.is_active =true and ts.is_active =true  THEN 'Continued_Playing'
    WHEN ts.is_active= true and ls.years_since_last_active>0 THEN 'Returned after Retirement'
     WHEN ls.is_active=false and ts.is_active=false THEN 'Stayed Retired'
  ELSE 'OTHER'
  END AS state_change
FROM
  last_season ls
  FULL OUTER JOIN this_season ts ON ls.player_name = ts.player_name
