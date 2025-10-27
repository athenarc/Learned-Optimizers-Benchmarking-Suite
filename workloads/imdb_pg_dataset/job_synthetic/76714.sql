SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  >  4 AND t.production_year  =  1952 AND ci.person_id  >  324092 AND ci.role_id  >  1