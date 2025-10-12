SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1987 AND ci.person_id  <  1245139 AND ci.role_id  <  10