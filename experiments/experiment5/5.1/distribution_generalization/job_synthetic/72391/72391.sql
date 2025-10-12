SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  2013 AND ci.person_id  <  229083 AND ci.role_id  =  1