SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.production_year  =  2004 AND ci.person_id  <  351422 AND ci.role_id  >  2 AND mk.keyword_id  <  14908