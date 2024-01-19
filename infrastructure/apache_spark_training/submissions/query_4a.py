%%sql
select distinct player_gamertag, match_id, 
AVG(player_total_kills) as avg_kills
from bootcamp.matchdetailsmedals
group by 1,2
order by 3 DESC
