SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1999 AND ci.person_id  >  1022532 AND ci.role_id  =  10