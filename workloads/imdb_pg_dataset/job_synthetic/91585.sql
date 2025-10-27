SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND ci.person_id  >  2051185 AND ci.role_id  <  8