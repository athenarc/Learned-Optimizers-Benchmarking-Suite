SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  <  7 AND t.production_year  <  1971 AND mc.company_id  >  109940 AND mc.company_type_id  =  2