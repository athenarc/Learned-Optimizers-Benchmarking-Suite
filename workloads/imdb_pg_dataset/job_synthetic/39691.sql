SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  3 AND mc.company_id  <  29376 AND ci.person_id  =  1333722 AND ci.role_id  <  5