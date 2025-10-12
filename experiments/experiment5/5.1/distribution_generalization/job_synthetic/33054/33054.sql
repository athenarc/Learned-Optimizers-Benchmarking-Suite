SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1932 AND ci.person_id  <  3609247 AND ci.role_id  <  2