SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  =  2012 AND ci.person_id  <  1419104 AND ci.role_id  >  1