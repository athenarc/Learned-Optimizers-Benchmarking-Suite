SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  <  2 AND t.production_year  =  1990 AND ci.person_id  <  3774536 AND ci.role_id  =  8