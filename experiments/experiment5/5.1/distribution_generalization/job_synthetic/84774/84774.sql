SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  >  1971 AND ci.person_id  <  835760 AND ci.role_id  =  10