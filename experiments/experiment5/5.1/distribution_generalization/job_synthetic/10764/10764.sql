SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.production_year  =  1987 AND mc.company_id  >  14266 AND mc.company_type_id  <  2