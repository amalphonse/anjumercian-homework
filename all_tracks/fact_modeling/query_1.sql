
--Query 1


INSERT INTO anjumercian.fct_nba_game_details
WITH
  cte AS (
      SELECT
        *,
        ROW_NUMBER() OVER(partition by game_id, team_id, player_id ORDER BY player_id) as rnk
      FROM
        anjumercian.fct_nba_game_details
      )
  
  select 
      game_id,
      team_id,
      player_id,
      dim_team_abbreviation, 
      dim_player_name,
      dim_start_position, 
      dim_did_not_dress, 
      dim_not_with_team, 
      m_seconds_played, 
      m_field_goals_made, 
      m_field_goals_attempted, 
      m_3_pointers_made, 
      m_3_pointers_attempted, 
      m_free_throws_made, 
      m_free_throws_attempted, 
      m_offensive_rebounds, 
      m_defensive_rebounds, 
      m_rebounds, 
      m_assists, 
      m_steals,
      m_blocks, 
      m_turnovers, 
      m_personal_fouls, 
      m_points, 
      m_plus_minus, 
      season, 
      team_did_win
  from cte
  where rnk=1
