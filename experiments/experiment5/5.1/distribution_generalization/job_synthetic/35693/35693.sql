SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  <  1999 AND mc.company_id  <  99189 AND mc.company_type_id  =  1