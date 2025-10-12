SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  >  4 AND t.production_year  <  2001 AND mc.company_type_id  >  1 AND ci.person_id  <  109822 AND ci.role_id  =  1