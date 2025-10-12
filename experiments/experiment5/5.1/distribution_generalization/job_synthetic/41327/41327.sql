SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  <  1977 AND ci.person_id  >  3060664 AND ci.role_id  =  8