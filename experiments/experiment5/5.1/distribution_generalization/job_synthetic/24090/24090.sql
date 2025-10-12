SELECT COUNT(*)
FROM title t, movie_info_idx mi_idx
WHERE t.id = mi_idx.movie_id AND t.kind_id  =  3 AND t.production_year  =  2003