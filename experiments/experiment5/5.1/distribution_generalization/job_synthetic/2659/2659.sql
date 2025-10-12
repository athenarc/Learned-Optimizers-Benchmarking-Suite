SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  7 AND t.production_year  >  1980 AND ci.person_id  >  2743242 AND ci.role_id  =  3