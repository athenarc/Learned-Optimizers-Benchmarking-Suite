SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  <  7 AND t.production_year  >  1978 AND mc.company_id  <  197203 AND mc.company_type_id  =  1 AND ci.person_id  <  494670 AND ci.role_id  =  10