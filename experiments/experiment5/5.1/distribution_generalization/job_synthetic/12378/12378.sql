SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  2001 AND ci.person_id  <  524265 AND ci.role_id  =  10