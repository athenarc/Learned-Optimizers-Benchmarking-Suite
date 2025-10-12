SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1958 AND ci.person_id  <  3452125 AND ci.role_id  =  4