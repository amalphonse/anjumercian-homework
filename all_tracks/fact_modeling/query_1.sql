#Query 1

INSERT INTO anjumercian.fct_nba_game_details
WITH
  yesterday AS (
    SELECT
      *
    FROM
      anjumercian.fct_nba_game_details
    WHERE
      game_date = '2023-08-01'
  ),
today AS (
    SELECT
      *
    FROM
      anjumercian.fct_nba_game_details
    WHERE
      game_date = '2023-08-01’'
  )
SELECT
  COALESCE(t.game_id, y.game_id) AS game_id,
  COALESCE(t.team_id, y.team_id) AS team_id,
COALESCE(t.player_id, y.player_id) AS player_id,
  t.dim_team_abbreviation, 
t.dim_player_name,
 t.dim_start_position, 
t.dim_did_not_dress, 
t.dim_not_with_team, 
t.m_seconds_played, 
t.m_field_goals_made, 
t.m_field_goals_attempted, 
t.m_3_pointers_made, 
t.m_3_pointers_attempted, 
t.m_free_throws_made, 
t.m_free_throws_attempted, 
t.m_offensive_rebounds, 
t.m_defensive_rebounds, 
t.m_rebounds, 
t.m_assists, 
t.m_steals,
 t.m_blocks, 
t.m_turnovers, 
t.m_personal_fouls, 
t.m_points, 
t.m_plus_minus, 
t.season, 
t.team_did_win ,
  '2023-08-01' AS game_date
FROM
  today t
 INNER JOIN yesterday y ON t.game_id = y.game_id
  AND t.team_id = y.team_id
AND t.player_id = y.player_id
