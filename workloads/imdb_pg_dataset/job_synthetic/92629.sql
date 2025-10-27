SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  =  1960 AND ci.person_id  <  3587235 AND ci.role_id  <  5 AND mi.info_type_id  <  61