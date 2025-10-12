SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  =  1977 AND mc.company_id  >  98239 AND mc.company_type_id  =  2