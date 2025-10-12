SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.kind_id  <  3 AND t.production_year  >  1928 AND mc.company_id  =  17380 AND mc.company_type_id  =  2