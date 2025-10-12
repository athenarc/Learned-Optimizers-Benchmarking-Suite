SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1915 AND ci.person_id  <  446777 AND ci.role_id  >  1