SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  <  1995 AND ci.person_id  <  2784156 AND ci.role_id  >  5