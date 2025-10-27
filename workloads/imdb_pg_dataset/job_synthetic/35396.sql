SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  >  2006 AND mc.company_id  <  686 AND mc.company_type_id  <  2 AND ci.person_id  >  1057825 AND ci.role_id  <  2