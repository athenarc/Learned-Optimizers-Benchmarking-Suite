SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  4 AND t.production_year  =  1997 AND mc.company_id  >  11141 AND ci.person_id  <  1975218 AND ci.role_id  =  1