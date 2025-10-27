SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  <  4 AND t.production_year  =  2003 AND mc.company_id  <  9401 AND mc.company_type_id  >  1