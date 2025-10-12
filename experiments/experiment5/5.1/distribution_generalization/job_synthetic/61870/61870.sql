SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  <  1913 AND ci.person_id  <  1797851 AND ci.role_id  >  3