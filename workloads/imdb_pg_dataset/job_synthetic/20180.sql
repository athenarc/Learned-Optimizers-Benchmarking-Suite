SELECT COUNT(*)
FROM title t, movie_info mi, movie_keyword mk
WHERE t.id = mi.movie_id AND t.id = mk.movie_id AND t.kind_id  =  7 AND t.production_year  <  1971 AND mi.info_type_id  >  16 AND mk.keyword_id  <  10914