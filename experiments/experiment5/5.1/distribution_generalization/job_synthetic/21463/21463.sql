SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1927 AND ci.person_id  >  3800601 AND ci.role_id  >  2