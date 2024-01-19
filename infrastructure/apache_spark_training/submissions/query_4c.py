%%sql

select distinct mapid, COUNT(map_name) 
from bootcamp.matchdetailsmedals
GROUP BY 1
order by 2 DESC
