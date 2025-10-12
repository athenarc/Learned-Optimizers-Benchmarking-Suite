SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.kind_id  >  3 AND ci.person_id  >  3104852 AND ci.role_id  <  10