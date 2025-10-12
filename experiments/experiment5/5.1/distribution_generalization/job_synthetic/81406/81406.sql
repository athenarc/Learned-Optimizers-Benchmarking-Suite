SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1949 AND ci.person_id  <  2423941 AND ci.role_id  >  1