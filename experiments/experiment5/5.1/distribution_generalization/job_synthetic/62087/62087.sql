SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  6 AND t.production_year  >  2000 AND ci.person_id  <  1524249 AND ci.role_id  >  10