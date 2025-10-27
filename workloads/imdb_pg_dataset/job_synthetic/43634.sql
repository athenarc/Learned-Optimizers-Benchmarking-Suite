SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  >  1 AND ci.person_id  >  384516 AND mi_idx.info_type_id  =  101