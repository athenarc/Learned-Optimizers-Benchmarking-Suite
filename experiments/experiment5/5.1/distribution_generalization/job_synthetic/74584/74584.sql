SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  =  1 AND t.production_year  =  1998 AND ci.person_id  <  1123767 AND ci.role_id  <  9 AND mk.keyword_id  >  14313