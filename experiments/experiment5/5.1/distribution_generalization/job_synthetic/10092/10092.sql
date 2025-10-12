SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  >  2 AND t.production_year  =  1991 AND mc.company_id  =  137