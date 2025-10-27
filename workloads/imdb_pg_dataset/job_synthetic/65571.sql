SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  >  1954 AND ci.person_id  <  2267548 AND ci.role_id  =  6