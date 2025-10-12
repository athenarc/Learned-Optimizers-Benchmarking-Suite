SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  <  1915 AND mc.company_id  >  202937 AND ci.person_id  <  1792875 AND ci.role_id  =  2