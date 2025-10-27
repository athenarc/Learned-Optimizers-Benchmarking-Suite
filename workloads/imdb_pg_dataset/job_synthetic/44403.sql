SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1933 AND ci.person_id  >  1469501 AND ci.role_id  <  3