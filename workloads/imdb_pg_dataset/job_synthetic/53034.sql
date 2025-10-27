SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1994 AND ci.person_id  <  1572160 AND ci.role_id  <  8