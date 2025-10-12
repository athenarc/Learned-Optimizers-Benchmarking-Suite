SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1963 AND ci.person_id  >  3020011 AND ci.role_id  >  2