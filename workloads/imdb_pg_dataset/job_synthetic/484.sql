SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  2005 AND ci.person_id  >  1410784 AND ci.role_id  >  2