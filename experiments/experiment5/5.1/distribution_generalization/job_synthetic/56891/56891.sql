SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.production_year  =  1967 AND mc.company_id  <  28186 AND mc.company_type_id  =  2 AND mi.info_type_id  <  2