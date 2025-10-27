SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.kind_id  =  1 AND t.production_year  =  1904 AND mk.keyword_id  <  121