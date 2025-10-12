SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  <  2008 AND ci.person_id  =  1869300 AND ci.role_id  <  3 AND mi.info_type_id  <  2