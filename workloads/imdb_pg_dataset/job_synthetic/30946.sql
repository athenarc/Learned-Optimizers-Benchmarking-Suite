SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.kind_id  =  7 AND t.production_year  >  2010 AND mc.company_id  <  73942 AND mc.company_type_id  <  2 AND mi.info_type_id  <  7