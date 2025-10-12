SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  <  3 AND t.production_year  >  1996 AND ci.person_id  <  2366041 AND ci.role_id  >  10