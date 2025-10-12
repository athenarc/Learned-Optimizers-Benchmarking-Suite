SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  <  3 AND t.production_year  =  2011 AND ci.person_id  >  2003602 AND ci.role_id  >  1