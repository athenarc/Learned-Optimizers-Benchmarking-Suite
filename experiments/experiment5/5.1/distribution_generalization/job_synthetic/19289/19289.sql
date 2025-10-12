SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1945 AND ci.person_id  <  612529 AND ci.role_id  =  8