SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  2004 AND ci.person_id  <  2334092 AND ci.role_id  =  2