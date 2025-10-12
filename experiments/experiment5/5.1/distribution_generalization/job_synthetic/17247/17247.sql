SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1968 AND ci.person_id  >  3856170 AND ci.role_id  >  2