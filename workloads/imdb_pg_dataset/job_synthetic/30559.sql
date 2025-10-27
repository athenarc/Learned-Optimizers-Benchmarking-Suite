SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  =  2 AND t.production_year  >  2004 AND ci.person_id  <  2796924 AND ci.role_id  <  5