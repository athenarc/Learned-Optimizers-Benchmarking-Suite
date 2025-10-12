SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  7 AND ci.person_id  >  2318705 AND ci.role_id  <  3