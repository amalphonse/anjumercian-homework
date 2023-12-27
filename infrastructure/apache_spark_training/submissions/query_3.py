#which player has the highest average kills per game? 

%%sql
select distinct player_gamertag, match_id, 
AVG(player_total_kills) OVER(partition by player_gamertag order by match_id) as avg_kills
from bootcamp.matchdetailsmedals
order by 3 DESC

player_gamertag	match_id	avg_kills
GsFurreal	acf0e47e-20ac-4b12-b292-591d4b3a3df9	56.0
zombiesrhunters	acf0e47e-20ac-4b12-b292-591d4b3a3df9	56.0
gimpinator14	acf0e47e-20ac-4b12-b292-591d4b3a3df9	56.0
WhiteMountainDC	acf0e47e-20ac-4b12-b292-591d4b3a3df9	56.0
hibblesnbitz	acf0e47e-20ac-4b12-b292-591d4b3a3df9	56.0
XoptimusprideX	acf0e47e-20ac-4b12-b292-591d4b3a3df9	56.0
taurenmonk	acf0e47e-20ac-4b12-b292-591d4b3a3df9	56.0


#which playlist has received the most plays? (query_4b)
%%sql

select playlist_id, COUNT(match_id) 
from bootcamp.matchdetailsmedals
GROUP BY 1
order by 2 DESC


playlist_id	count(match_id)
f72e0ef0-7c4a-4307-af78-8e38dac3fdba	1565529

#which map was played the most?

%%sql

select distinct mapid, COUNT(mapid) 
from bootcamp.matchdetailsmedals
GROUP BY 1
order by 2 DESC

mapid	count(mapid)
c74c9d0f-f206-11e4-8330-24be05e24f7e	1445545

#on which map do players receive the highest number of Killing Spree medals?

%%sql

select distinct mapid, COUNT(player_total_kills) 
from bootcamp.matchdetailsmedals
GROUP BY 1
order by 2 DESC

mapid	count(player_total_kills)
c74c9d0f-f206-11e4-8330-24be05e24f7e	1445545


