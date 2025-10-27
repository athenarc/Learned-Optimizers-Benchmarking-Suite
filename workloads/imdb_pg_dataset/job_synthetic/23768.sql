SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  =  3 AND ci.person_id  <  2766665 AND ci.role_id  <  3 AND mk.keyword_id  =  411