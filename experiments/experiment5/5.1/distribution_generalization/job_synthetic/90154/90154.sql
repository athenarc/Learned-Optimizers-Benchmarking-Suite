SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  <  1964 AND ci.person_id  <  3834689 AND ci.role_id  =  1