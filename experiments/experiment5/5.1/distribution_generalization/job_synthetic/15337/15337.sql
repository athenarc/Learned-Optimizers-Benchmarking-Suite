SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.kind_id  =  1 AND t.production_year  >  2003 AND mc.company_id  <  1639 AND mk.keyword_id  <  64380