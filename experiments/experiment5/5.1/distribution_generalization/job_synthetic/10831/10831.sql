SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1993 AND ci.person_id  <  1253428 AND ci.role_id  >  2