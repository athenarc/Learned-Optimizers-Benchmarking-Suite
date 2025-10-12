SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.kind_id  <  4 AND t.production_year  =  1964 AND mc.company_id  >  50833 AND mc.company_type_id  >  1