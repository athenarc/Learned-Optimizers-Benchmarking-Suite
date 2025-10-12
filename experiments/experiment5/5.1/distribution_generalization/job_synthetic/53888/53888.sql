SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  >  1998 AND mc.company_id  <  1402 AND mc.company_type_id  =  2