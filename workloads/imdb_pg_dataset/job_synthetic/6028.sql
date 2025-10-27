SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  =  1930 AND mc.company_id  <  21846 AND mc.company_type_id  <  2