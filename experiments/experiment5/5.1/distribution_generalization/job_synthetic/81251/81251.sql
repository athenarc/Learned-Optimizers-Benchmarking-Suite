SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1983 AND ci.person_id  <  3055433 AND ci.role_id  >  10