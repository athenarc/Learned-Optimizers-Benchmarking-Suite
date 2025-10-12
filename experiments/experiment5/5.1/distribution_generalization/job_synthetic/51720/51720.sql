SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  =  7 AND ci.person_id  <  193399 AND ci.role_id  >  4 AND mk.keyword_id  >  18897