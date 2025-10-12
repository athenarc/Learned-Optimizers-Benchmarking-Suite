SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  =  1 AND t.production_year  =  1966 AND mc.company_id  >  3928 AND mc.company_type_id  <  2