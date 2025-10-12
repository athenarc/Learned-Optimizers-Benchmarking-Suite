SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1986 AND ci.person_id  <  740497 AND ci.role_id  <  10