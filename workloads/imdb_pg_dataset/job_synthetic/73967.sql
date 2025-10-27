SELECT COUNT(*)
FROM title t, movie_keyword mk
WHERE t.id = mk.movie_id AND t.kind_id  =  1 AND t.production_year  <  1933 AND mk.keyword_id  <  659