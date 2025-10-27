SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  >  1 AND t.production_year  =  1986 AND ci.person_id  >  2111378 AND ci.role_id  <  4 AND mi_idx.info_type_id  >  99