SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND mc.company_id  >  2561 AND mc.company_type_id  =  1 AND mi.info_type_id  <  16