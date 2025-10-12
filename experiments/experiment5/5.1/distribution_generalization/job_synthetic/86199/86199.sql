SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  >  2010 AND mc.company_id  <  6101 AND mc.company_type_id  >  1 AND ci.person_id  <  451486 AND ci.role_id  >  1