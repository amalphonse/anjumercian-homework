%%sql

select playlist_id, COUNT(match_id) 
from bootcamp.matchdetailsmedals
GROUP BY 1
order by 2 DESC
