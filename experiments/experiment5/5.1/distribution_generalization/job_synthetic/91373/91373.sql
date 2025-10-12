SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1992 AND ci.person_id  <  4042283 AND ci.role_id  >  3