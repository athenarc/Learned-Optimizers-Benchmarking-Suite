SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  >  2 AND ci.person_id  >  1066249 AND ci.role_id  =  3