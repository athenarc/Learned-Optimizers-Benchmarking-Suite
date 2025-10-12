SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.production_year  =  1947 AND mc.company_id  <  12929 AND mi.info_type_id  <  8