SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  <  4 AND ci.person_id  >  2648170 AND ci.role_id  <  11 AND mi.info_type_id  >  54