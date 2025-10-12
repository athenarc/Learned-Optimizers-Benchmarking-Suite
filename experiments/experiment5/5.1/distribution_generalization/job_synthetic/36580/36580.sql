SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1972 AND ci.person_id  =  876691 AND ci.role_id  >  3