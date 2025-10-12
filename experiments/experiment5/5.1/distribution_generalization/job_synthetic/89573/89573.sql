SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  4 AND t.production_year  <  2010 AND ci.person_id  >  2957230 AND ci.role_id  >  10