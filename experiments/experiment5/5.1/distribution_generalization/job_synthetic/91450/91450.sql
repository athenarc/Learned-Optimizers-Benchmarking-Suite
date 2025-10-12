SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  0 AND ci.person_id  =  244241 AND ci.role_id  >  1