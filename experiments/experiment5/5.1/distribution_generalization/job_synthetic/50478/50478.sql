SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  =  3 AND t.production_year  >  1988 AND mc.company_id  <  12471 AND mc.company_type_id  >  1