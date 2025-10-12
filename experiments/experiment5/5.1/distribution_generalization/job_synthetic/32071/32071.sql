SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  >  4 AND t.production_year  =  1962 AND ci.role_id  <  2