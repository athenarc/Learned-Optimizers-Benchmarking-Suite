SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  >  1 AND t.production_year  <  1995 AND mc.company_id  >  9658 AND mc.company_type_id  =  2 AND ci.person_id  >  252390