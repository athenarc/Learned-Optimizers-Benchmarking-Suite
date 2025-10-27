SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  <  7 AND ci.person_id  <  1674620 AND mi.info_type_id  >  7