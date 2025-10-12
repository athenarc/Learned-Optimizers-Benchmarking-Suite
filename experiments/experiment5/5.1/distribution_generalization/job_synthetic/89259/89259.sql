SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.production_year  <  1984 AND ci.person_id  =  942074 AND ci.role_id  <  9 AND mi_idx.info_type_id  =  100