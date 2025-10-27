SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1974 AND ci.person_id  >  2879940 AND ci.role_id  >  1