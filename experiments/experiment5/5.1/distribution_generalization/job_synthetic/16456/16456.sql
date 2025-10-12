SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1998 AND ci.person_id  >  2181016 AND ci.role_id  >  4