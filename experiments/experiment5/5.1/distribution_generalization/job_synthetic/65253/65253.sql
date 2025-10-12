SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  <  2 AND t.production_year  <  1997 AND ci.person_id  <  3182818 AND ci.role_id  >  1