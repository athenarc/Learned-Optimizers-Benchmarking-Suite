SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1929 AND ci.person_id  >  2665489 AND ci.role_id  >  4