SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1964 AND ci.person_id  <  319674 AND ci.role_id  <  2