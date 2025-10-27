SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND ci.person_id  <  839989 AND ci.role_id  =  2