SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  =  1998 AND mc.company_id  =  85125 AND ci.person_id  >  3415685