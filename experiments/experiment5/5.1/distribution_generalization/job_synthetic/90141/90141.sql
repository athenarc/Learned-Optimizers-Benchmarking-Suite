SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  2009 AND ci.person_id  <  667558 AND ci.role_id  =  1