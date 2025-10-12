SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  <  7 AND t.production_year  =  1975 AND mc.company_id  <  73838 AND mc.company_type_id  =  1