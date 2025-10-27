SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  =  7 AND t.production_year  <  2012 AND ci.person_id  <  1383022 AND mi_idx.info_type_id  >  100