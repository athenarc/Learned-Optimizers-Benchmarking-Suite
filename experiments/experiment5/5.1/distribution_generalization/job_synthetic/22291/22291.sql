SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  >  1993 AND mc.company_id  <  11713 AND ci.person_id  >  1775348 AND ci.role_id  <  4