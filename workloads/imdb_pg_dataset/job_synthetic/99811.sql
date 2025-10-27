SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  <  7 AND t.production_year  <  2010 AND mc.company_id  >  239 AND mc.company_type_id  =  2 AND ci.person_id  <  1765386