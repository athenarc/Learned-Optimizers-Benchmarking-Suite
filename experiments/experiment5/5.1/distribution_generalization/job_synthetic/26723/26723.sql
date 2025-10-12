SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  <  7 AND t.production_year  >  1954 AND mc.company_id  <  100490 AND mc.company_type_id  <  2 AND ci.role_id  =  2