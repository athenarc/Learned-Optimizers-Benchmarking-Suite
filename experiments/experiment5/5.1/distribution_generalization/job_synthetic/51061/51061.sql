SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1981 AND ci.person_id  <  1874422 AND ci.role_id  =  3