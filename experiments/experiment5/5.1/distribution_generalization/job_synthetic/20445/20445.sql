SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  =  1 AND t.production_year  >  1989 AND mc.company_id  >  5488 AND mc.company_type_id  <  2