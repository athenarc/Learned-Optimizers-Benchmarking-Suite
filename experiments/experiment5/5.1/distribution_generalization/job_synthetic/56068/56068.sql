SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  2007 AND ci.person_id  <  1657623 AND ci.role_id  <  3