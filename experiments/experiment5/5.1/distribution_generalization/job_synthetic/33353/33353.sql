SELECT COUNT(*)
FROM title t, movie_info mi, movie_info_idx mi_idx
WHERE t.id = mi.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  =  7 AND t.production_year  >  1953 AND mi.info_type_id  >  15 AND mi_idx.info_type_id  =  99