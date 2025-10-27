SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND ci.person_id  =  2332591 AND ci.role_id  <  3