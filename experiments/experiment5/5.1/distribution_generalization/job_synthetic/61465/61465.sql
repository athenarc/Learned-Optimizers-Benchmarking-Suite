SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  >  2011 AND mc.company_id  =  28437 AND ci.person_id  <  2717878 AND ci.role_id  <  8