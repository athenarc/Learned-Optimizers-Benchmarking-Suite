SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1985 AND ci.person_id  =  520423 AND ci.role_id  <  2