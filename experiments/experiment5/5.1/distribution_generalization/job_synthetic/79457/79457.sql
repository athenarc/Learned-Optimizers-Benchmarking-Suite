SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  2006 AND ci.person_id  <  439142 AND ci.role_id  =  1