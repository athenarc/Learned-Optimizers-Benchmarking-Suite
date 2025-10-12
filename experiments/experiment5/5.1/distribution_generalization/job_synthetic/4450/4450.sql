SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  =  7 AND t.production_year  =  1983 AND ci.person_id  <  2521283 AND ci.role_id  >  2 AND mk.keyword_id  <  355