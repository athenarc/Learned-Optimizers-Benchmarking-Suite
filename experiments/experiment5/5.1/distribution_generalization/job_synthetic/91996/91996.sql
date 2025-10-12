SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.production_year  >  1943 AND mc.company_id  <  21194 AND mi.info_type_id  =  5