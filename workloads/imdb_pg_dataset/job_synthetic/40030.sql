SELECT COUNT(*)
FROM title t, movie_info mi, movie_keyword mk
WHERE t.id = mi.movie_id AND t.id = mk.movie_id AND t.kind_id  >  6 AND t.production_year  =  1994 AND mk.keyword_id  <  5363