SELECT COUNT(*)
FROM title t, movie_keyword mk
WHERE t.id = mk.movie_id AND t.kind_id  =  4 AND t.production_year  =  2007 AND mk.keyword_id  >  382