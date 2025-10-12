SELECT COUNT(*)
FROM title t, movie_info_idx mi_idx, movie_keyword mk
WHERE t.id = mi_idx.movie_id AND t.id = mk.movie_id AND t.kind_id  >  1 AND mi_idx.info_type_id  >  100 AND mk.keyword_id  >  68724