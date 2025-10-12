SELECT COUNT(*)
FROM title t, movie_info mi, movie_info_idx mi_idx
WHERE t.id = mi.movie_id AND t.id = mi_idx.movie_id AND t.production_year  <  2011 AND mi.info_type_id  <  18 AND mi_idx.info_type_id  >  99