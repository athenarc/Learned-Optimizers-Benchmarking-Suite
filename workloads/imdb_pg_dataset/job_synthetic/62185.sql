SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  2002 AND ci.person_id  <  3812008 AND ci.role_id  =  9