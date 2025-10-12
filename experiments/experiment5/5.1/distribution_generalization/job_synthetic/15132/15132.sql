SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  4 AND mc.company_id  <  24112 AND ci.person_id  >  2879584 AND ci.role_id  =  10