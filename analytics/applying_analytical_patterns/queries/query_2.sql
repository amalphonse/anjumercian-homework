SELECT
 gd.player_name,
  team_id,
  season,
  COUNT(1)
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

