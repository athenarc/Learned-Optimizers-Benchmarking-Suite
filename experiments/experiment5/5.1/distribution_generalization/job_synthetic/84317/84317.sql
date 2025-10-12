SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  <  3 AND t.production_year  >  1984 AND mc.company_id  =  4628 AND mc.company_type_id  >  1