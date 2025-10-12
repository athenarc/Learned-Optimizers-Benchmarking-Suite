SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  >  2 AND t.production_year  >  2004 AND ci.person_id  >  599631