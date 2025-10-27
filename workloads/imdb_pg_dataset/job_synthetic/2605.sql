SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  <  7 AND t.production_year  =  1991 AND ci.person_id  <  982252 AND ci.role_id  =  3 AND mi.info_type_id  <  16