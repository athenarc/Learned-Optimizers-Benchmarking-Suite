SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  7 AND ci.person_id  <  2987682 AND ci.role_id  >  2