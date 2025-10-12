SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.production_year  =  2005 AND mc.company_type_id  <  2 AND mk.keyword_id  <  24834