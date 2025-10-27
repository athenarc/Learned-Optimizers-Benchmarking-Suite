SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  <  2008 AND mc.company_id  >  22290 AND mc.company_type_id  =  2 AND ci.person_id  =  2374252 AND ci.role_id  <  3