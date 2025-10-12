SELECT COUNT(*)
FROM title t, movie_info mi, movie_keyword mk
WHERE t.id = mi.movie_id AND t.id = mk.movie_id AND t.production_year  =  1989 AND mi.info_type_id  >  8 AND mk.keyword_id  <  75864