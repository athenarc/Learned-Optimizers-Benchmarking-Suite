SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  >  1987 AND ci.person_id  >  1275957 AND ci.role_id  =  2