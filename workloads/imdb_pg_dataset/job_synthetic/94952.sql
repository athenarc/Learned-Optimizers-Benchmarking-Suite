SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  <  2009 AND mc.company_id  <  3496 AND ci.person_id  <  2346528 AND ci.role_id  >  1