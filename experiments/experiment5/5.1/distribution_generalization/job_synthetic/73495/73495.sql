SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1991 AND ci.person_id  >  3992224 AND ci.role_id  >  1