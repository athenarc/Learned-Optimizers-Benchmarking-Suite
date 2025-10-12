SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  2012 AND ci.person_id  >  851625 AND ci.role_id  >  5