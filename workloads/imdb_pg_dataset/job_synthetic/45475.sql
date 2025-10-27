SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND ci.person_id  >  2411944 AND ci.role_id  >  1