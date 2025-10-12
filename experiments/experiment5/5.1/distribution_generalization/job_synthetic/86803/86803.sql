SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  >  1956 AND mc.company_id  <  15198 AND mc.company_type_id  =  2