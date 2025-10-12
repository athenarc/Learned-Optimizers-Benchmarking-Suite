SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1897 AND ci.person_id  <  653289 AND ci.role_id  =  1