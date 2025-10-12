SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  >  1960 AND mc.company_id  >  12117 AND mc.company_type_id  =  1