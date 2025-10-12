SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  3 AND mc.company_id  >  14921 AND ci.person_id  <  1130321 AND ci.role_id  =  1