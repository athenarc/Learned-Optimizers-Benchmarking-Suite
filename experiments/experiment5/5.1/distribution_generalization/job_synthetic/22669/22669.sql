SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  <  4 AND ci.person_id  =  2924166 AND ci.role_id  >  9