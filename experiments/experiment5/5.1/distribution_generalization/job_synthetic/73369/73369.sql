SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  2011 AND ci.person_id  <  362322 AND ci.role_id  =  1