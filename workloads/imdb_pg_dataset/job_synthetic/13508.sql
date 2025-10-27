SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  <  7 AND t.production_year  =  2008 AND ci.person_id  >  85298 AND mk.keyword_id  <  22343