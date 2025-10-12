SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  =  1982 AND ci.person_id  <  123523 AND ci.role_id  <  9