SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  <  1978 AND ci.person_id  <  1840071 AND ci.role_id  =  3