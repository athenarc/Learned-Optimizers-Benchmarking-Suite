SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  >  1906 AND ci.person_id  <  1492142 AND ci.role_id  >  1