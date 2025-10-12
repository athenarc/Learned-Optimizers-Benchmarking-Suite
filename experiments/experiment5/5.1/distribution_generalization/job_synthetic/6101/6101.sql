SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  >  1996 AND ci.role_id  <  9 AND mi.info_type_id  =  3