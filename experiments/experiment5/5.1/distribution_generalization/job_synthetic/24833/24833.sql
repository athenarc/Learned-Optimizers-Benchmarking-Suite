SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  =  1 AND t.production_year  <  1899 AND ci.person_id  <  973557 AND ci.role_id  >  8