SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  <  1999 AND mc.company_id  <  154880 AND ci.person_id  =  2941322 AND ci.role_id  >  1