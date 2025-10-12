SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.production_year  >  1990 AND ci.person_id  =  585439 AND ci.role_id  <  2