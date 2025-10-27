SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  >  1998 AND mc.company_id  <  11428 AND mc.company_type_id  >  1 AND ci.person_id  <  909713 AND ci.role_id  <  2