SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1996 AND ci.person_id  =  3502946 AND ci.role_id  >  6