SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  <  7 AND t.production_year  >  2010 AND ci.person_id  <  1545279 AND mk.keyword_id  =  335