
SELECT
 player_name,
  team_id,
  game_id,
  SUM(gd.pts) OVER (partition BY game_id order by team_id)
FROM
  bootcamp.nba_game_details gd
