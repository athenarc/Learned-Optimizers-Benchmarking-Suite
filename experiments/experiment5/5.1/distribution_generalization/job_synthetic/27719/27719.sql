SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  =  2005 AND mc.company_id  =  253 AND mc.company_type_id  =  2