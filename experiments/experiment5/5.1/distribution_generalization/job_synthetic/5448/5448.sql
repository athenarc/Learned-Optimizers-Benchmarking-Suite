SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  <  2010 AND ci.person_id  =  214660 AND ci.role_id  <  5