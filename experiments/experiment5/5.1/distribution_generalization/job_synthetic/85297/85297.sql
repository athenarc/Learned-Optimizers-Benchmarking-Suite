SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  <  2006 AND mc.company_id  <  2267 AND mc.company_type_id  >  1 AND ci.person_id  =  3206822 AND ci.role_id  <  5