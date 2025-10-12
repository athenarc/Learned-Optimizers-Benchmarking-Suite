SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  >  2007 AND ci.person_id  <  989028 AND ci.role_id  <  2