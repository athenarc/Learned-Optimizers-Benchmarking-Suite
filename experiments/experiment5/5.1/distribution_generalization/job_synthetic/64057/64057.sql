SELECT COUNT(*)
FROM title t, movie_info_idx mi_idx, movie_keyword mk
WHERE t.id = mi_idx.movie_id AND t.id = mk.movie_id AND t.production_year  <  1968 AND mi_idx.info_type_id  =  101 AND mk.keyword_id  <  825