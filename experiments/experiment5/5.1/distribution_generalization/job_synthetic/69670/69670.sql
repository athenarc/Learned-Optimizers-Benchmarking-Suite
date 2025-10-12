SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  <  7 AND t.production_year  <  1968 AND ci.person_id  >  3034213 AND ci.role_id  =  10