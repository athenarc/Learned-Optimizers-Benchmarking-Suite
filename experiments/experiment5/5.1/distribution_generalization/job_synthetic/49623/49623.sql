SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.kind_id  =  7 AND t.production_year  =  2004 AND mc.company_id  <  23113 AND mc.company_type_id  =  1 AND mk.keyword_id  >  231