SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  2010 AND ci.person_id  <  2496209 AND ci.role_id  =  9