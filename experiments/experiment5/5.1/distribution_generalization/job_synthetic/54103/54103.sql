SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1968 AND ci.person_id  >  800979 AND ci.role_id  <  10