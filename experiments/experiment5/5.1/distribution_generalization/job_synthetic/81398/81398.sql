SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  >  1913 AND ci.person_id  <  958730 AND mi.info_type_id  =  4