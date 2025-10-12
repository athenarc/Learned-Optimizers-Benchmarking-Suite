SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.production_year  >  1932 AND mc.company_id  =  36079 AND mc.company_type_id  <  2 AND mi.info_type_id  =  2