SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  >  4 AND ci.person_id  =  1666709 AND ci.role_id  <  4