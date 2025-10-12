SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1999 AND ci.person_id  <  2506141 AND ci.role_id  =  2