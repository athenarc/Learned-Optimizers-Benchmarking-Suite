SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  2002 AND ci.person_id  <  257610 AND ci.role_id  <  3