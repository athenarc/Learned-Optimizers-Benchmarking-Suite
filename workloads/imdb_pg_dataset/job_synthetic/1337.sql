SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  =  1 AND ci.person_id  <  2062413 AND ci.role_id  <  2