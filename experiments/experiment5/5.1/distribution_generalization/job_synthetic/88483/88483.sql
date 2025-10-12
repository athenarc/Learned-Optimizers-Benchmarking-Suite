SELECT COUNT(*)
FROM title t, cast_info ci, movie_info_idx mi_idx
WHERE t.id = ci.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  <  4 AND t.production_year  =  2009 AND ci.person_id  <  2935163 AND ci.role_id  =  1