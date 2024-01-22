SELECT
 gd.player_name,
  team_id,
  season,
  SUM(gd.pts) as total_points
FROM
  bootcamp.nba_game_details gd
  JOIN bootcamp.nba_player_seasons s
  ON gd.player_name=s.player_name
GROUP BY
  GROUPING SETS (
    (gd.player_name, team_id),
    (gd.player_name, season),
    (team_abbreviation)
  )
