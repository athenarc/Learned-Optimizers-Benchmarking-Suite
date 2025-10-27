SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  <  7 AND ci.person_id  >  100267 AND ci.role_id  =  2