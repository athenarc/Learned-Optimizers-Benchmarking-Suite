SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.production_year  >  1971 AND mc.company_id  <  89198 AND mc.company_type_id  =  1