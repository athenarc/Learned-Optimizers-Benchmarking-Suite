SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  <  7 AND t.production_year  <  1985 AND mc.company_id  =  2182 AND mc.company_type_id  <  2 AND ci.person_id  <  2027944 AND ci.role_id  <  2