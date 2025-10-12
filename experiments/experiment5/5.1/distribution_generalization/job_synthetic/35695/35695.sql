SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND mc.company_id  =  13015 AND ci.person_id  >  559241 AND ci.role_id  <  4