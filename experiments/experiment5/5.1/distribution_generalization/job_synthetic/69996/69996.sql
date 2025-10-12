SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  >  1988 AND mc.company_id  =  144178 AND mc.company_type_id  =  2