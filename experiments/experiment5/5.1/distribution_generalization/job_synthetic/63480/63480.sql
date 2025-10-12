SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  >  2010 AND ci.person_id  >  3125165 AND ci.role_id  >  3