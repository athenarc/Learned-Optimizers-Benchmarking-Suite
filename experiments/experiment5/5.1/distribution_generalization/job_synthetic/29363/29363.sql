SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  >  1991 AND mc.company_id  >  131407 AND mc.company_type_id  =  2