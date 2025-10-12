SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1987 AND ci.person_id  <  431543 AND ci.role_id  <  6