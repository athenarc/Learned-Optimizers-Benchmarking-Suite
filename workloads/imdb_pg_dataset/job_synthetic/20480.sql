SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  =  1 AND t.production_year  >  1990 AND ci.person_id  >  2275124 AND mk.keyword_id  <  2375