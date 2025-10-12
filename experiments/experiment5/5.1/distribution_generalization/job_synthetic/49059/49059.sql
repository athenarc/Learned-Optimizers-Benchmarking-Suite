SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  >  1 AND t.production_year  >  2008 AND ci.person_id  <  2872191 AND ci.role_id  =  2 AND mi_idx.info_type_id  <  101