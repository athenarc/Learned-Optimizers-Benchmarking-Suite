SELECT COUNT(*)
FROM title t, movie_info mi, movie_keyword mk
WHERE t.id = mi.movie_id AND t.id = mk.movie_id AND t.kind_id  <  3 AND t.production_year  =  1998 AND mi.info_type_id  =  7 AND mk.keyword_id  =  4754