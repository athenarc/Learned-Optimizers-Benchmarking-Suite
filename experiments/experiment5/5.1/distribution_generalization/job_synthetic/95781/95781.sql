SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND ci.person_id  =  1755821 AND ci.role_id  =  3 AND mk.keyword_id  <  40156