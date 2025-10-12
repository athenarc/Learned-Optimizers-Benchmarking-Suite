SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  2 AND t.production_year  <  2008 AND ci.person_id  <  903101 AND ci.role_id  <  6