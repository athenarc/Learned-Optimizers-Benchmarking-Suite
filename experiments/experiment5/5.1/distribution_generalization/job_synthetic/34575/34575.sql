SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  <  3 AND t.production_year  >  1970 AND mc.company_id  >  73 AND ci.person_id  >  2918645 AND ci.role_id  >  3