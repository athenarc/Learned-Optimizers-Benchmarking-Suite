SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  >  1 AND ci.person_id  >  2865508 AND ci.role_id  =  8 AND mk.keyword_id  =  16264