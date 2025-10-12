SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  <  7 AND ci.person_id  <  2694239 AND ci.role_id  >  9 AND mi.info_type_id  =  16