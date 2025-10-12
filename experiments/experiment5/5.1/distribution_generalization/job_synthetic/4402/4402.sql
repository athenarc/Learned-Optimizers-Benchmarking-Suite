SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1920 AND ci.person_id  >  226677 AND ci.role_id  <  10