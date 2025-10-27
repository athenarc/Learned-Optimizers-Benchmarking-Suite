SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  >  2008 AND mc.company_id  >  180257 AND mc.company_type_id  =  2 AND ci.person_id  <  1443300 AND ci.role_id  =  2