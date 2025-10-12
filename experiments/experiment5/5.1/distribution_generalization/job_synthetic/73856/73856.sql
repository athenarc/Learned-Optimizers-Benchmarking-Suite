SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  >  2008 AND ci.person_id  <  2549817 AND ci.role_id  <  5