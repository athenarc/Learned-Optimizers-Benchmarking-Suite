SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1976 AND ci.person_id  >  769073 AND ci.role_id  =  5