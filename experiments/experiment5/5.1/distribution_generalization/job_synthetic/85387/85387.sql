SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.kind_id  <  4 AND t.production_year  >  1973 AND mc.company_id  <  11662 AND mk.keyword_id  =  382