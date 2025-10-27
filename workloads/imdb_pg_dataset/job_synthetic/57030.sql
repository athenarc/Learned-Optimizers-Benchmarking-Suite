SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  <  2011 AND mc.company_id  =  19004 AND ci.person_id  <  2036512 AND ci.role_id  <  11