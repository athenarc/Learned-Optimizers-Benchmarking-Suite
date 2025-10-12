SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.production_year  <  2004 AND mc.company_id  <  673 AND mk.keyword_id  =  11426