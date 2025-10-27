SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  <  2 AND t.production_year  <  2002 AND ci.person_id  =  2719092 AND ci.role_id  >  1