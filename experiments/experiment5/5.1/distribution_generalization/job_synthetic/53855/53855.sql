SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND mc.company_id  >  12948 AND mc.company_type_id  >  1 AND ci.person_id  =  3000482 AND ci.role_id  <  8