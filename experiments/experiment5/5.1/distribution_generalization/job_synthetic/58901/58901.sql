SELECT COUNT(*)
FROM title t, movie_info mi, movie_keyword mk
WHERE t.id = mi.movie_id AND t.id = mk.movie_id AND t.kind_id  =  1 AND mi.info_type_id  <  98 AND mk.keyword_id  =  77699