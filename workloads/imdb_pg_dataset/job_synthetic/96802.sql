SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  4 AND t.production_year  >  1954 AND ci.person_id  >  3051721 AND ci.role_id  =  3