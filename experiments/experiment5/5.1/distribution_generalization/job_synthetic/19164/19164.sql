SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1997 AND ci.person_id  <  2418391 AND ci.role_id  <  2