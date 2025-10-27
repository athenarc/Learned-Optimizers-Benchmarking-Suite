SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.production_year  >  1972 AND ci.person_id  =  95397 AND ci.role_id  <  4 AND mi_idx.info_type_id  >  100