SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  =  1954 AND mc.company_id  >  16655 AND ci.person_id  >  3047117