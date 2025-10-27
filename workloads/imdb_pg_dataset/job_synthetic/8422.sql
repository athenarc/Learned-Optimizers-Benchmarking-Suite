SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  <  1974 AND mc.company_id  >  419 AND mc.company_type_id  =  1