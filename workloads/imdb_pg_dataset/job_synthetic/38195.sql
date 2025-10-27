SELECT COUNT(*)
FROM title t, movie_keyword mk
WHERE t.id = mk.movie_id AND t.kind_id  =  7 AND mk.keyword_id  =  28956