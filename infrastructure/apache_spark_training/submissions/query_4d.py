%%sql

select distinct mapid, COUNT(player_total_kills) 
from bootcamp.matchdetailsmedals
GROUP BY 1
order by 2 DESC
