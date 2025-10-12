SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1956 AND ci.person_id  <  185187 AND ci.role_id  =  6