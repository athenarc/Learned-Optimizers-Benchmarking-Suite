SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  <  2 AND t.production_year  =  1981 AND mc.company_id  <  11379 AND mc.company_type_id  =  1