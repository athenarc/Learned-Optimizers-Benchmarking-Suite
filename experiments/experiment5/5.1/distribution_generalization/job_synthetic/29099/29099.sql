SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  1946 AND ci.person_id  >  509636 AND ci.role_id  >  3