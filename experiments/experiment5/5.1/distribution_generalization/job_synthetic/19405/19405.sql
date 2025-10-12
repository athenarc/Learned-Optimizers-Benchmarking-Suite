SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  <  3 AND t.production_year  =  1966 AND ci.person_id  >  508038 AND ci.role_id  <  10